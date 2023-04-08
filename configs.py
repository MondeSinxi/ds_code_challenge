from configparser import ConfigParser

configur = ConfigParser()
configur.read(".secrets/credentials")

S3_ACCESS_KEY = configur.get("default", "aws_access_key_id")
S3_SECRET_ACCESS_KEY = configur.get("default", "aws_secret_access_key")
S3_REGION = configur.get("default", "region_name")
S3_ENDPOINT = f"s3.{S3_REGION}.com"

DUCKDB_INIT = f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_access_key_id='{S3_ACCESS_KEY}';
    SET s3_secret_access_key='{S3_SECRET_ACCESS_KEY}';
    SET s3_region='{S3_REGION}';
    SET s3_url_style='path';
    SET s3_endpoint='s3.{S3_REGION}.amazonaws.com';
"""
WIND_DATA_URL = "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"
