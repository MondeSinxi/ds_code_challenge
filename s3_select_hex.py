import boto3
import json
from validation import Feature
from typing import List
import logging

logging.basicConfig(level=logging.INFO)

s3 = boto3.client("s3", region_name="af-south-1")

BUCKET = "cct-ds-code-challenge-input-data"
KEY = "city-hex-polygons-8-10.geojson"
query = "SELECT * FROM s3object[*].features[*] s WHERE s.properties.resolution = 8"


def get_records() -> List[Feature]:
    """S3 query that returns level 8 resolution polygons."""
    resp = s3.select_object_content(
        Bucket=BUCKET,
        Key=KEY,
        ExpressionType="SQL",
        Expression=query,
        InputSerialization={"JSON": {"Type": "Document"}},
        OutputSerialization={"JSON": {}},
    )

    records = ""
    for event in resp["Payload"]:
        if "Records" in event:
            logging.debug(f"Fetched record payload: {event['Records']['Payload']}")
            records += event["Records"]["Payload"].decode("utf-8")
    features = list(map(json.loads, records.split()))
    feature_models = [Feature(**feature) for feature in features]
    logging.info(f"Collected {len(feature_models)} from S3 SELECT query")
    return feature_models


def get_validated_data() -> List[Feature]:
    logging.info("Fetching valid geojson data for comparison")
    with open("data/city-hex-polygons-8.geojson", "r") as file:
        validation_data = json.loads(file.read())
    validation_features = [
        Feature(**feature) for feature in validation_data["features"]
    ]
    sorted_validation_features = sorted(
        validation_features, key=lambda f: f.properties.index
    )
    for i, feature in enumerate(sorted_validation_features):
        sorted_validation_features[i].properties.resolution = 8
    logging.info(f"Collected {len(sorted_validation_features)} records from valid data")
    return sorted_validation_features


def main():
    features = get_records()
    sorted_features = sorted(features, key=lambda f: f.properties.index)
    valid_data = get_validated_data()

    if sorted_features == valid_data:
        logging.info("The collected data from S3 SELECT Query is valid!")
    else:
        raise Exception("Data collected from query is not valid!")


if __name__ == "__main__":
    main()
