import boto3
import json
import pandas as pd
from typing import List
import logging
from src.utils import timing
from src.validation import Feature

logging.basicConfig(level=logging.INFO)


BUCKET = "cct-ds-code-challenge-input-data"
KEY = "city-hex-polygons-8-10.geojson"
query = "SELECT * FROM s3object[*].features[*] s WHERE s.properties.resolution = 8"


class GeoQuery:
    def __init__(self, query=query, region_name="af-south-1"):
        self.s3 = boto3.client("s3", region_name=region_name)
        self.query = query
        self.get_records()

    @timing
    def get_records(self) -> List[Feature]:
        """S3 query that returns level 8 resolution polygons."""
        resp = self.s3.select_object_content(
            Bucket=BUCKET,
            Key=KEY,
            ExpressionType="SQL",
            Expression=self.query,
            InputSerialization={"JSON": {"Type": "Document"}},
            OutputSerialization={"JSON": {}},
        )

        results = ""
        for event in resp["Payload"]:
            if "Records" in event:
                logging.debug(f"Fetched record payload: {event['Records']['Payload']}")
                results += event["Records"]["Payload"].decode("utf-8")
        self.create_models(results)
        return

    def records_df(self):
        return pd.DataFrame.from_dict(
            {
                "index": [r.properties.index for r in self.records],
                "latitude": [r.properties.centroid_lat for r in self.records],
                "longitude": [r.properties.centroid_lon for r in self.records],
            },
            orient="columns",
        )

    def create_models(self, results: str):
        features = list(map(json.loads, results.split()))
        # Performs Schema validation and creates data models
        feature_models = [Feature(**feature) for feature in features]
        self.records = sorted(feature_models, key=lambda f: f.properties.index)
        logging.info(f"Collected {len(feature_models)} from S3 SELECT query")
        return

    def get_validated_data(self) -> List[Feature]:
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
        logging.info(
            f"Collected {len(sorted_validation_features)} records from valid data"
        )
        return sorted_validation_features

    @property
    def is_valid(self) -> bool:
        valid_data = self.get_validated_data()
        if self.records == valid_data:
            return True
        return False


if __name__ == "__main__":
    query = "SELECT * FROM s3object[*].features[*] s WHERE s.properties.resolution = 8"
    gq = GeoQuery(query)
