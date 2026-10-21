import os
import re
import csv
import json
import time
import argparse
import itertools
from pathlib import Path
from collections import Counter

import boto3
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

bucket = "hh-lake"
source_key = "raw/vacancies_2024.csv"
java_pattern = re.compile(r"(^|[^a-z])java([^a-z]|$)|джав")

csv.field_size_limit(2**31 - 1)

def physical_lines(chunk_size, size, stats):
    pending = b""
    first_line = True

    def decode_line(value):
        nonlocal first_line
        encoding = "utf-8-sig" if first_line else "utf-8"
        first_line = False
        return value.decode(encoding)

    for start in range(0, size, chunk_size):
        end = min(start + chunk_size, size) - 1
        response = s3.get_object(
            Bucket=bucket,
            Key=source_key,
            Range=f"bytes={start}-{end}",
        )
        data = response["Body"].read()
        expected = end - start + 1
        if len(data) != expected:
            raise IOError(f"Range length mismatch at byte {start}: {len(data)} != {expected}")

        stats["bytes_read"] += len(data)
        stats["range_requests"] += 1
        parts = data.split(b"\n")

        if len(parts) == 1:
            pending += data
            continue

        yield decode_line(pending + parts[0] + b"\n")

        for part in parts[1:-1]:
            yield decode_line(part + b"\n")

        pending = parts[-1]

    if pending:
        yield decode_line(pending)

def normalize_skills(value):
    if not value:
        return []
    return sorted({
        " ".join(skill.strip().split()).casefold()
        for skill in value.split(";")
        if skill.strip()
    })

def run(chunk_mib):
    chunk_size = chunk_mib * 1024 * 1024
    object_info = s3.head_object(Bucket=bucket, Key=source_key)
    object_size = object_info["ContentLength"]
    stats = {"bytes_read": 0, "range_requests": 0}
    singles = Counter()
    pairs = Counter()
    triples = Counter()
    source_rows = 0
    matching_rows = 0
    rows_with_skills = 0
    malformed_rows = 0
    started = time.perf_counter()

    reader = csv.reader(
        physical_lines(chunk_size, object_size, stats),
        strict=True,
    )
    headers = next(reader)
    indexes = {name.strip().lstrip("\ufeff"): i for i, name in enumerate(headers)}

    required = ("role", "name", "key_skills", "vacancy_id")
    missing = [name for name in required if name not in indexes]
    if missing:
        raise ValueError(f"Required CSV columns missing: {missing}")

    role_i = indexes["role"]
    name_i = indexes["name"]
    skills_i = indexes["key_skills"]
    id_i = indexes["vacancy_id"]
    max_i = max(role_i, name_i, skills_i, id_i)

    for row in reader:
        if not row:
            continue
        source_rows += 1

        if len(row) <= max_i:
            malformed_rows += 1
            continue

        if row[role_i] != "Программист, разработчик":
            continue

        if not java_pattern.search(row[name_i].lower()):
            continue

        matching_rows += 1
        skills = normalize_skills(row[skills_i])
        if not skills:
            continue

        rows_with_skills += 1
        singles.update(skills)
        pairs.update(itertools.combinations(skills, 2))
        triples.update(itertools.combinations(skills, 3))

    elapsed = time.perf_counter() - started

    result = {
        "method": "python_s3_range_mapreduce",
        "chunk_size_mib": chunk_mib,
        "source_object": f"s3://{bucket}/{source_key}",
        "object_size_bytes": object_size,
        "bytes_read": stats["bytes_read"],
        "range_requests": stats["range_requests"],
        "source_rows": source_rows,
        "processed_rows": matching_rows,
        "rows_with_skills": rows_with_skills,
        "malformed_rows": malformed_rows,
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

    local = Path("results") / f"mapreduce_s3_range_{chunk_mib}mib.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(json.dumps(result, ensure_ascii=False))

    key = f"results/mapreduce_s3_range_{chunk_mib}mib.json"
    s3.upload_file(str(local), bucket, key)

    print(json.dumps({
        "chunk_size_mib": chunk_mib,
        "object_size_bytes": object_size,
        "bytes_read": stats["bytes_read"],
        "range_requests": stats["range_requests"],
        "source_rows": source_rows,
        "java_vacancies": matching_rows,
        "rows_with_skills": rows_with_skills,
        "malformed_rows": malformed_rows,
        "distinct_skills": len(singles),
        "distinct_pairs": len(pairs),
        "distinct_triples": len(triples),
        "elapsed_seconds": round(elapsed, 3),
        "saved_to": f"s3://{bucket}/{key}",
    }, ensure_ascii=False), flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=[64, 32, 8])
    args = parser.parse_args()

    for size in args.sizes:
        if size <= 0:
            raise ValueError("Chunk sizes must be positive")
        run(size)

if __name__ == "__main__":
    main()
