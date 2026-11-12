import os
from pathlib import Path

import boto3
import duckdb
from botocore.config import Config

root = Path(__file__).resolve().parent
project = root.parent

for line in (project / ".env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

destination = root / "input" / "java_vacancies.parquet"
destination.parent.mkdir(parents=True, exist_ok=True)

s3.download_file(
    "hh-lake",
    "processed/java_vacancies.parquet",
    str(destination),
)

con = duckdb.connect()
rows = con.execute(
    "SELECT key_skills FROM read_parquet(?)",
    [str(destination)],
).fetchall()

with_skills = sum(
    1 for (skills,) in rows
    if skills is not None and str(skills).strip()
)

print("Input:", destination)
print("Vacancies:", len(rows))
print("With skills:", with_skills)
print("Without skills:", len(rows) - with_skills)

assert len(rows) == 6100
assert with_skills == 4088

print("Input validation passed")
