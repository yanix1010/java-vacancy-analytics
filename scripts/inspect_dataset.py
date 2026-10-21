import csv
import io
import json
import os
import re
from collections import Counter
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

response = client.get_object(
    Bucket="hh-lake",
    Key="raw/vacancies_2024.csv",
)

stream = io.TextIOWrapper(response["Body"], encoding="utf-8-sig", newline="")
reader = csv.DictReader(stream)

limit = 100_000
statuses = Counter()
areas = Counter()
roles = Counter()
currencies = Counter()
java_names = Counter()
examples = []
date_min = None
date_max = None
rows = 0
skill_rows = 0

java_pattern = re.compile(r"java|джав", re.IGNORECASE)

try:
    for row in reader:
        rows += 1

        status = (row.get("status") or "").strip()
        area = (row.get("area") or "").strip()
        role = (row.get("role") or "").strip()
        name = (row.get("name") or "").strip()
        date = (row.get("snapshot_date") or "").strip()
        skills = (row.get("key_skills") or "").strip()
        currency = (row.get("currency") or "").strip()

        statuses[status or "<empty>"] += 1
        areas[area or "<empty>"] += 1
        roles[role or "<empty>"] += 1
        currencies[currency or "<empty>"] += 1

        if skills:
            skill_rows += 1

        if date:
            date_min = date if date_min is None else min(date_min, date)
            date_max = date if date_max is None else max(date_max, date)

        if java_pattern.search(name):
            java_names[name] += 1
            if len(examples) < 20:
                examples.append({
                    "name": name,
                    "role": role,
                    "status": status,
                    "area": area,
                    "snapshot_date": date,
                    "key_skills": skills[:250],
                })

        if rows >= limit:
            break
finally:
    stream.close()

print(json.dumps({
    "sample_rows": rows,
    "columns": reader.fieldnames,
    "statuses": statuses.most_common(),
    "date_min": date_min,
    "date_max": date_max,
    "rows_with_key_skills": skill_rows,
    "top_areas": areas.most_common(15),
    "top_roles": roles.most_common(15),
    "currencies": currencies.most_common(),
    "java_name_matches_in_sample": sum(java_names.values()),
    "distinct_java_names_in_sample": len(java_names),
    "java_name_examples": examples,
}, ensure_ascii=False, indent=2))
