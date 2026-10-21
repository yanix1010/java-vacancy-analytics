import os
import json
import time
import itertools
from pathlib import Path
from collections import Counter

import boto3
import duckdb
from botocore.config import Config

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and "=" in line:
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

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")
con.execute(f"""
CREATE SECRET rustfs_secret (
    TYPE S3,
    KEY_ID '{os.environ["RUSTFS_ACCESS_KEY"]}',
    SECRET '{os.environ["RUSTFS_SECRET_KEY"]}',
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
)
""")

source = "s3://hh-lake/processed/java_vacancies.parquet"

def normalize_skills(value):
    if not value:
        return []
    return sorted({
        " ".join(skill.strip().split()).casefold()
        for skill in value.split(";")
        if skill.strip()
    })

def run(batch_size):
    singles = Counter()
    pairs = Counter()
    triples = Counter()
    processed = 0
    rows_with_skills = 0
    started = time.perf_counter()

    cursor = con.execute(
        "SELECT vacancy_id, key_skills FROM read_parquet(?)",
        [source],
    )

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        for vacancy_id, raw_skills in rows:
            processed += 1
            skills = normalize_skills(raw_skills)
            if not skills:
                continue

            rows_with_skills += 1
            singles.update(skills)
            pairs.update(itertools.combinations(skills, 2))
            triples.update(itertools.combinations(skills, 3))

    elapsed = time.perf_counter() - started

    result = {
        "method": "python_mapreduce",
        "batch_size": batch_size,
        "processed_rows": processed,
        "rows_with_skills": rows_with_skills,
        "distinct_skills": len(singles),
        "distinct_pairs": len(pairs),
        "distinct_triples": len(triples),
        "elapsed_seconds": round(elapsed, 3),
        "single_skill_counts": [
            {"skill": skill, "count": count}
            for skill, count in singles.most_common()
        ],
        "pair_counts": [
            {"skills": list(skills), "count": count}
            for skills, count in pairs.most_common()
        ],
        "triple_counts": [
            {"skills": list(skills), "count": count}
            for skills, count in triples.most_common()
        ],
    }

    local = Path("results") / f"mapreduce_batch_{batch_size}.json"
    local.parent.mkdir(exist_ok=True)
    local.write_text(json.dumps(result, ensure_ascii=False))

    key = f"results/mapreduce_batch_{batch_size}.json"
    s3.upload_file(str(local), "hh-lake", key)

    print(json.dumps({
        "batch_size": batch_size,
        "processed_rows": processed,
        "rows_with_skills": rows_with_skills,
        "distinct_skills": len(singles),
        "distinct_pairs": len(pairs),
        "distinct_triples": len(triples),
        "elapsed_seconds": round(elapsed, 3),
        "saved_to": f"s3://hh-lake/{key}",
    }, ensure_ascii=False), flush=True)

for size in [256, 1024, 4096]:
    run(size)
