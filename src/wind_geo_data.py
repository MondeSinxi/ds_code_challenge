import urllib.request
import math
import numpy as np
from pathlib import Path
import pandas as pd
import duckdb
import logging
from typing import Tuple

from src.configs import WIND_DATA_URL
from src.s3_select_hex import GeoQuery
from src.utils import generate_array_of_randoms, timing

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


def filter_by_minute(suburb: str = "BELLVILLE SOUTH") -> pd.DataFrame:
    df_srv_hex = pd.read_parquet("data/sr_hex.parquet")
    df_srv_hex_deg_min = duckdb.query(
        """
        select *,
        floor(longitude)::integer long_degree,
        floor(60*(longitude - long_degree))::integer long_min,
        ceil(latitude)::integer lat_degree,
	abs(floor(60*(latitude - lat_degree)))::integer lat_min,
        from df_srv_hex
        """
    ).df()

    lat_centroid, long_centroid = get_suburb_centroid(suburb)
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


@timing
def join_wind_to_service(suburb: str):
    srv_hex_deg_min = filter_by_minute(suburb)
    df_wind = get_winds_data()
    logging.info("Joining wind data to service data...")
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
          * exclude ("Date & Time", round_datetime, long_degree, lat_degree, long_min, lat_min, long_diff, lat_diff, lon_diff_min, lat_diff_min)
        from service_tab
        left join winds on
            service_tab.round_datetime = winds."Date & Time"
        """
    ).df()


def anonymise_coordinates(
    latitude: pd.Series, longitude: pd.Series, within_max_distance=500
) -> Tuple[pd.Series, pd.Series]:
    # 1 second latitude ~= 31 metres; 1 sec = 1/3600 deg
    # 1 second longitude ~= 25 metres; 1 sec = 1/3600 deg

    lat_max_abs_var = (within_max_distance / 31) / 3600
    lon_max_abs_var = (within_max_distance / 25) / 3600
    # generate randomness
    n = len(latitude)
    randoms = np.array(generate_array_of_randoms(n))
    mrandoms = 1 - randoms

    lat_variance = (2 * lat_max_abs_var * randoms) - lat_max_abs_var
    lon_variance = (2 * lon_max_abs_var * mrandoms) - lon_max_abs_var

    return latitude + lat_variance, longitude + lon_variance


def anonymise(service_data: pd.DataFrame) -> pd.DataFrame:
    service_data["latitude"], service_data["longitude"] = anonymise_coordinates(
        service_data["latitude"], service_data["longitude"]
    )
    return service_data
