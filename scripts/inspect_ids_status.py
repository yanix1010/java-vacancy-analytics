import csv
import io
import json
import os
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

statuses = Counter()
rows = 0
ids_missing = 0
java_role_status = Counter()
java_role_dates = Counter()
id_counts = Counter()
status_by_role = Counter()

try:
    for row in reader:
        rows += 1
        vacancy_id = (row.get("vacancy_id") or "").strip()
        status = (row.get("status") or "").strip()
        name = (row.get("name") or "").strip()
        role = (row.get("role") or "").strip()
        date = (row.get("snapshot_date") or "").strip()

        statuses[status or "<empty>"] += 1

        if not vacancy_id:
            ids_missing += 1
        else:
            id_counts[vacancy_id] += 1

        if role == "Программист, разработчик":
            java_role_status[status or "<empty>"] += 1
            java_role_dates[date or "<empty>"] += 1

        status_by_role[(role or "<empty>", status or "<empty>")] += 1
finally:
    stream.close()

duplicate_ids = sum(1 for count in id_counts.values() if count > 1)
duplicate_extra_rows = sum(count - 1 for count in id_counts.values() if count > 1)

print(json.dumps({
    "rows_scanned": rows,
    "unique_vacancy_ids": len(id_counts),
    "ids_with_multiple_rows": duplicate_ids,
    "extra_rows_for_repeated_ids": duplicate_extra_rows,
    "missing_ids": ids_missing,
    "statuses": statuses.most_common(),
    "programmer_developer_statuses": java_role_status.most_common(),
    "latest_snapshot_dates_in_programmer_role": java_role_dates.most_common(20),
    "top_role_status_combinations": [
        {"role": role, "status": status, "count": count}
        for (role, status), count in status_by_role.most_common(30)
    ]
}, ensure_ascii=False, indent=2))
