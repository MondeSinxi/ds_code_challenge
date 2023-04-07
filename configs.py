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
