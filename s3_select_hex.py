import boto3

s3 = boto3.client("s3", region_name="af-south-1")

BUCKET = "cct-ds-code-challenge-input-data"
KEY = "city-hex-polygons-8-10.geojson"
query = "SELECT * FROM s3object[*].features[*] s WHERE s.properties.resolution = 8"

resp = s3.select_object_content(
    Bucket=BUCKET,
    Key=KEY,
    ExpressionType="SQL",
    Expression=query,
    InputSerialization={"JSON": {"Type": "Document"}},
    OutputSerialization={"JSON": {}},
)

for event in resp["Payload"]:
    if "Records" in event:
        records = event["Records"]["Payload"].decode("utf-8")
        print(records)
