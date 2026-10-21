import os
import time
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

source = Path("/mnt/c/Users/User/Downloads/vacancies_2024.csv")
bucket = "hh-lake"
key = "raw/vacancies_2024.csv"

if not source.is_file():
    raise FileNotFoundError(source)

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
)

config = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=2,
    use_threads=True,
)

size = source.stat().st_size
print(f"Source size: {size:,} bytes", flush=True)
print(f"Destination: s3://{bucket}/{key}", flush=True)

started = time.perf_counter()
client.upload_file(
    str(source),
    bucket,
    key,
    Config=config,
)

elapsed = time.perf_counter() - started
result = client.head_object(Bucket=bucket, Key=key)
uploaded_size = result["ContentLength"]

print(f"Uploaded size: {uploaded_size:,} bytes")
print(f"Size matches: {uploaded_size == size}")
print(f"Elapsed seconds: {elapsed:.2f}")
print(f"Average MiB/s: {size / 1024 / 1024 / elapsed:.2f}")
print(f"Object: s3://{bucket}/{key}")

if uploaded_size != size:
    raise RuntimeError("Uploaded object size does not match source")
