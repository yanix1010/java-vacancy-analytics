import os
import json
from pathlib import Path

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

source = "'s3://hh-lake/processed/java_vacancies.parquet'"

con.execute(f"""
CREATE TEMP TABLE normalized AS
SELECT
    vacancy_id,
    list_sort(
        list_distinct(
            list_filter(
                list_transform(
                    string_split(coalesce(key_skills, ''), ';'),
                    x -> lower(trim(regexp_replace(x, '\\s+', ' ', 'g')))
                ),
                x -> x <> ''
            )
        )
    ) AS skills
FROM read_parquet({source})
""")

single_rows = con.execute("""
SELECT skill, count(*) AS frequency
FROM (
    SELECT unnest(skills) AS skill
    FROM normalized
    WHERE len(skills) > 0
)
GROUP BY skill
ORDER BY skill
""").fetchall()

pair_rows = con.execute("""
SELECT skill_a, skill_b, count(*) AS frequency
FROM (
    SELECT
        list_extract(skills, i) AS skill_a,
        list_extract(skills, j) AS skill_b
    FROM normalized,
         unnest(range(1, len(skills) + 1)) AS a(i),
         unnest(range(1, len(skills) + 1)) AS b(j)
    WHERE i < j
)
GROUP BY skill_a, skill_b
ORDER BY skill_a, skill_b
""").fetchall()

triple_rows = con.execute("""
SELECT skill_a, skill_b, skill_c, count(*) AS frequency
FROM (
    SELECT
        list_extract(skills, i) AS skill_a,
        list_extract(skills, j) AS skill_b,
        list_extract(skills, k) AS skill_c
    FROM normalized,
         unnest(range(1, len(skills) + 1)) AS a(i),
         unnest(range(1, len(skills) + 1)) AS b(j),
         unnest(range(1, len(skills) + 1)) AS c(k)
    WHERE i < j AND j < k
)
GROUP BY skill_a, skill_b, skill_c
ORDER BY skill_a, skill_b, skill_c
""").fetchall()

sql_result = {
    "method": "duckdb_sql",
    "single_skill_counts": [
        {"skill": skill, "count": count}
        for skill, count in single_rows
    ],
    "pair_counts": [
        {"skills": [a, b], "count": count}
        for a, b, count in pair_rows
    ],
    "triple_counts": [
        {"skills": [a, b, c], "count": count}
        for a, b, c, count in triple_rows
    ],
}

result_path = Path("results/duckdb_sql.json")
result_path.parent.mkdir(exist_ok=True)
result_path.write_text(json.dumps(sql_result, ensure_ascii=False))

s3.upload_file(str(result_path), "hh-lake", "results/duckdb_sql.json")

checks = []
for batch_size in [256, 1024, 4096]:
    mr_path = Path(f"results/mapreduce_batch_{batch_size}.json")
    mr = json.loads(mr_path.read_text())

    def single_map(items):
        return {x["skill"]: x["count"] for x in items}

    def combo_map(items):
        return {tuple(x["skills"]): x["count"] for x in items}

    equal_singles = single_map(mr["single_skill_counts"]) == single_map(sql_result["single_skill_counts"])
    equal_pairs = combo_map(mr["pair_counts"]) == combo_map(sql_result["pair_counts"])
    equal_triples = combo_map(mr["triple_counts"]) == combo_map(sql_result["triple_counts"])

    checks.append({
        "batch_size": batch_size,
        "singles_match": equal_singles,
        "pairs_match": equal_pairs,
        "triples_match": equal_triples,
        "all_match": equal_singles and equal_pairs and equal_triples,
    })

print(json.dumps({
    "duckdb_distinct_skills": len(single_rows),
    "duckdb_distinct_pairs": len(pair_rows),
    "duckdb_distinct_triples": len(triple_rows),
    "checks": checks,
    "sql_result_object": "s3://hh-lake/results/duckdb_sql.json",
}, ensure_ascii=False, indent=2))
