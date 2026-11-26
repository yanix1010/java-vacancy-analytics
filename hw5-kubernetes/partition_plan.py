import csv
import json
import os

import boto3
from botocore.config import Config

BUCKET = "hh-lake"
KEY = "raw/vacancies_2024.csv"
PARTITIONS = 4
CHUNK = 8 * 1024 * 1024

csv.field_size_limit(2**31 - 1)

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["RUSTFS_ENDPOINT"],
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

size = s3.head_object(Bucket=BUCKET, Key=KEY)["ContentLength"]
position = 0
first = True

def physical_lines():
    global position, first
    pending = b""

    for start in range(0, size, CHUNK):
        end = min(start + CHUNK, size) - 1
        response = s3.get_object(
            Bucket=BUCKET,
            Key=KEY,
            Range=f"bytes={start}-{end}",
        )
        body = response["Body"]
        try:
            data = body.read()
        finally:
            body.close()

        assert len(data) == end - start + 1
        parts = data.split(b"\n")

        if len(parts) == 1:
            pending += data
            continue

        raw = pending + parts[0] + b"\n"
        position += len(raw)
        yield raw.decode("utf-8-sig" if first else "utf-8")
        first = False

        for part in parts[1:-1]:
            raw = part + b"\n"
            position += len(raw)
            yield raw.decode("utf-8")

        pending = parts[-1]

    if pending:
        position += len(pending)
        yield pending.decode("utf-8-sig" if first else "utf-8")

reader = csv.reader(physical_lines(), strict=True)
header = next(reader)
targets = [size * i // PARTITIONS for i in range(1, PARTITIONS)]
boundaries = [0]
rows = 0

for row in reader:
    if not row:
        continue

    rows += 1

    if len(boundaries) <= len(targets):
        target = targets[len(boundaries) - 1]
        if position >= target:
            boundaries.append(position)
            print("BOUNDARY:", len(boundaries) - 1, position, flush=True)

    if rows % 1000000 == 0:
        print("ROWS_SCANNED:", rows, flush=True)

boundaries.append(size)

assert position == size
assert len(boundaries) == PARTITIONS + 1
assert all(a < b for a, b in zip(boundaries, boundaries[1:]))

plan = {
    "bucket": BUCKET,
    "key": KEY,
    "object_size_bytes": size,
    "source_rows": rows,
    "partitions": PARTITIONS,
    "boundaries": boundaries,
    "header": header,
}

s3.put_object(
    Bucket=BUCKET,
    Key="hw5/partition_plan.json",
    Body=json.dumps(plan, ensure_ascii=False).encode("utf-8"),
    ContentType="application/json",
)

print("SOURCE_ROWS:", rows)
print("PARTITIONS:", PARTITIONS)
print("BOUNDARIES:", boundaries)
print("PARTITION_PLAN: PASSED", flush=True)
