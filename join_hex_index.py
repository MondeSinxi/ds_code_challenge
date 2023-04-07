"""Testing for threshold"""
from s3_select_hex import GeoQuery
import duckdb
import pandas as pd
import logging
from configs import DUCKDB_INIT


def run_threshold_test(threshold=0.0001, n=10, delta=0.0001, limit=100000):
    logging.info("Initializing DuckDB with local S3 settings...")
    duckdb.query(DUCKDB_INIT)
    with open("sql/test_threshold.sql", "r") as sql_file:
        test_threshold = sql_file.read()

    with open("sql/error_missed_join.sql", "r") as sql_file:
        error_missed_join = sql_file.read()

    with open("sql/error_loose_threshold.sql", "r") as sql_file:
        error_loose_threshold = sql_file.read()

    gq = GeoQuery()

    if not gq.is_valid:
        raise Exception("GEOJSON data is not valid")

    df = gq.records_df()
    report = pd.DataFrame({})

    for i in range(n):
        threshold += delta
        logging.info(f"Running Analysis for Threshold={threshold}")
        dbq = duckdb.query(test_threshold.format(threshold=threshold, limit=limit))

        missed_join_df = duckdb.query(error_missed_join).to_df()
        loose_threshold_df = duckdb.query(error_loose_threshold).to_df()
        total_df = duckdb.query("select count(*) AS total from dbq where latitude is not null and longitude is not null").to_df()

        error_df = pd.concat([missed_join_df, loose_threshold_df, total_df], axis=1)
        error_df["threshold"] = threshold
        report = pd.concat([report, error_df])
    report["coverage"] = 1 - report["missed_joins"]/report["total"]
    logging.info(report.reset_index())

if __name__ == "__main__":
    run_threshold_test()
