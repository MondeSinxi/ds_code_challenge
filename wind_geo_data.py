import urllib.request
from configs import WIND_DATA_URL
import math
import numpy as np
from pathlib import Path
import pandas as pd
import duckdb
from s3_select_hex import GeoQuery
import logging
from typing import Tuple

filename = "data/wind_data.ods"


def get_winds_data() -> pd.DataFrame:
    if not Path(filename).exists():
        logging.info(f"Fetching data from {WIND_DATA_URL}")
        urllib.request.urlretrieve(WIND_DATA_URL, "data/wind_data.ods")

    df = pd.read_excel(filename, skiprows=2, header=[0, 1, 2])

    # fix column names
    df.columns = [
        "  ".join(c).strip() if "Date" not in c[0] else c[0] for c in df.columns
    ]

    # Data ends at this point, filter all data out after this index
    stop_index = df[df["Date & Time"] == "Minimum"].index[0]
    df = df.iloc[:stop_index]
    df["Date & Time"] = pd.to_datetime(df["Date & Time"], format="%d/%m/%Y %H:%M")
    # Replace NoData with None type
    df = df.replace("NoData", None)
    return df


def get_suburb_centroid(suburb: str = "BELLVILLE SOUTH") -> Tuple[float, float]:
    """Returns centroid for a suburb"""
    # get H3 codes for given suburb
    h3_index_query_result = duckdb.query(
        f"select distinct h3_level8_index from 'data/sr_hex.parquet' where official_suburb like '{suburb}'"
    ).fetchall()
    geo_indexes = [h[0] for h in h3_index_query_result]

    gqr = GeoQuery()

    longitude_centroids = [
        gqr.properties.centroid_lon
        for gqr in gqr.records
        if gqr.properties.index in geo_indexes
    ]

    long_centroid = np.array(longitude_centroids).mean()

    latitude_centroids = [
        gqr.properties.centroid_lat
        for gqr in gqr.records
        if gqr.properties.index in geo_indexes
    ]

    lat_centroid = np.array(latitude_centroids).mean()
    return (lat_centroid, long_centroid)


def convert_to_deg_min_sec(val):
    min_deg, deg = math.modf(val)
    deg_sec, minu = math.modf(min_deg * 60)
    sec = round(60 * deg_sec)
    return deg, abs(minu), abs(sec)


def filter_by_minute():
    df_srv_hex = pd.read_parquet("data/sr_hex.parquet")
    df_srv_hex_deg_min = duckdb.query(
        """
        select *,
        floor(longitude)::integer long_degree,
        floor(60*(longitude - floor(longitude)))::integer long_min,
        ceil(latitude)::integer lat_degree,
	abs(floor(60*(latitude - ceil(latitude))))::integer lat_min
        from df_srv_hex
        """
    ).df()

    lat_centroid, long_centroid = get_suburb_centroid()
    lat_degree, lat_min, lat_sec = convert_to_deg_min_sec(lat_centroid)
    long_degree, long_min, long_sec = convert_to_deg_min_sec(long_centroid)

    df_srv_hex_deg_min["long_diff"] = df_srv_hex_deg_min["long_degree"] - long_degree
    df_srv_hex_deg_min["lat_diff"] = df_srv_hex_deg_min["lat_degree"] - lat_degree
    df_srv_hex_deg_min["lat_diff_min"] = df_srv_hex_deg_min["lat_min"] - lat_min
    df_srv_hex_deg_min["lon_diff_min"] = df_srv_hex_deg_min["long_min"] - long_min

    return df_srv_hex_deg_min[
        (abs(df_srv_hex_deg_min["long_diff"]) == 0)
        & (abs(df_srv_hex_deg_min["lat_diff"]) == 0)
        & (abs(df_srv_hex_deg_min["lon_diff_min"]) < 2)
        & (abs(df_srv_hex_deg_min["lat_diff_min"]) < 2)
    ]


def join_wind_to_service():
    srv_hex_deg_min = filter_by_minute()
    df_wind = get_winds_data()

    return duckdb.query(
        """
        with service_tab as (
        select
          time_bucket(INTERVAL '1 hour', creation_timestamp) as round_datetime,
          *,
        from  srv_hex_deg_min),
        winds as (
        select
          "Date & Time",
          "Bellville South AQM Site  Wind Dir V  Deg" as wind_direction_degrees,
          "Bellville South AQM Site  Wind Speed V  m/s" as wind_speed_metres_per_second,
        from df_wind)
        select
          * exclude ("Date & Time", round_datetime)
        from service_tab
        left join winds on
            service_tab.round_datetime = winds."Date & Time"
        """
    ).df()
