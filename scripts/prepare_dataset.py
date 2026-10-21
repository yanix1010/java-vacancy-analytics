import os
import time
from pathlib import Path

import duckdb

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

def sql_string(value):
    return "'" + value.replace("'", "''") + "'"

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

con.execute(f"""
CREATE OR REPLACE SECRET rustfs_secret (
    TYPE S3,
    KEY_ID {sql_string(os.environ["RUSTFS_ACCESS_KEY"])},
    SECRET {sql_string(os.environ["RUSTFS_SECRET_KEY"])},
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
)
""")

source = "s3://hh-lake/raw/vacancies_2024.csv"
destination = "s3://hh-lake/processed/java_vacancies.parquet"

query = f"""
SELECT
    vacancy_id,
    status,
    snapshot_date,
    published_at,
    area,
    name,
    role,
    salary_from,
    salary_to,
    currency,
    gross,
    experience,
    schedule,
    employment,
    key_skills
FROM read_csv(
    {sql_string(source)},
    header = true,
    all_varchar = true,
    sample_size = 2048,
    parallel = true
)
WHERE role = 'Программист, разработчик'
  AND regexp_matches(
      lower(name),
      '(^|[^a-z])java([^a-z]|$)|джав'
  )
"""

started = time.perf_counter()
con.execute(f"""
COPY ({query})
TO {sql_string(destination)}
(FORMAT PARQUET, COMPRESSION ZSTD)
""")
elapsed = time.perf_counter() - started

count, distinct_ids, active_count, missing_skills = con.execute(f"""
SELECT
    count(*),
    count(DISTINCT vacancy_id),
    count(*) FILTER (WHERE status = 'active'),
    count(*) FILTER (WHERE key_skills IS NULL OR trim(key_skills) = '')
FROM read_parquet({sql_string(destination)})
""").fetchone()

print(f"Prepared rows: {count}")
print(f"Distinct vacancy IDs: {distinct_ids}")
print(f"Active rows: {active_count}")
print(f"Rows without skills: {missing_skills}")
print(f"Preparation seconds: {elapsed:.2f}")
print(f"Output: {destination}")
