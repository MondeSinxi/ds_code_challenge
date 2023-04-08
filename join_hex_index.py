"""Testing for threshold"""
from s3_select_hex import GeoQuery
import duckdb
import pandas as pd
from pathlib import Path
import logging
from configs import DUCKDB_INIT
from utils import timing


def get_hex_data():
    gq = GeoQuery()
    if not gq.is_valid:
        raise Exception("GEOJSON data is not valid")
    return gq.records_df()


def get_coverage(df: pd.DataFrame):
    logging.info("Checking coverage...")
    total_rows = df.shape[0]
    covered = df[(df["h3_level8_index"] != "0") & (df["latitude"].notna())].shape[0]
    missed = df[(df["h3_level8_index"] == "0") & (df["latitude"].notna())].shape[0]
    coverage = covered / total_rows
    return missed, coverage


def run_checks(sr_hex: pd.DataFrame, testing=False):
    logging.info("Run checks against reference data...")
    df = pd.read_parquet("data/sr_hex.parquet")
    errors = duckdb.query(
        """
        with joint as (
        select
            df.notification_number as ref,
            sr_hex.notification_number,
            df.h3_level8_index as h3_ref,
            sr_hex.h3_level8_index as h3
        from df left join sr_hex on
        df.notification_number = sr_hex.notification_number
        )
        select count(*) as errors
        from joint
        where h3_ref != h3
        """
    ).fetchall()[0][0]
    total_rows = df.shape[0]
    hit_rate = 1 - errors / total_rows
    missed, coverage = get_coverage(sr_hex)
    logging.info(
        f"After checks, the coverage is {coverage} with a total of {missed} missed joins"
    )
    logging.info(f"Similarity check against reference data is {hit_rate}")
    if not testing:
        assert (
            hit_rate > 0.9
        ), f"Hit rate of {hit_rate} is less than the threshold for passing tests"
    return hit_rate


@timing
def join_geodata(threshold=0.005):
    # Initialize DuckDB
    duckdb.query(DUCKDB_INIT)
    with open("sql/join_hex.sql", "r") as sql_file:
        join_hex_sql = sql_file.read()
    df = get_hex_data()
    logging.info("Joining GEOJSON data to service data...")
    return duckdb.query(join_hex_sql.format(threshold=threshold)).df()


def test_thresholds(iterations=10, threshold=0.001, delta=0.0005):
    hit_rates = []
    thresholds = []
    for i in range(iterations):
        sr_hex = join_geodata(threshold=threshold)
        hit_rates.append(run_checks(sr_hex, testing=True))
        thresholds.append(threshold)
        threshold += delta
    return pd.DataFrame({"similarity": hit_rates, "threshold": thresholds})


if __name__ == "__main__":
    test_thresholds()
#    run_threshold_test(n=10, threshold=0.001)
