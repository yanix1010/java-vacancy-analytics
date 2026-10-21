import os
from pathlib import Path

import boto3
from botocore.config import Config

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

bucket = "hh-lake"
buckets = [item["Name"] for item in client.list_buckets()["Buckets"]]

if bucket not in buckets:
    client.create_bucket(Bucket=bucket)

print("Buckets:", [item["Name"] for item in client.list_buckets()["Buckets"]])
print("Bucket verification:", client.head_bucket(Bucket=bucket)["ResponseMetadata"]["HTTPStatusCode"])
