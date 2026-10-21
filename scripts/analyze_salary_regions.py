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

def q(value):
    return "'" + value.replace("'", "''") + "'"

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")
con.execute(f"""
CREATE SECRET rustfs_secret (
    TYPE S3,
    KEY_ID {q(os.environ["RUSTFS_ACCESS_KEY"])},
    SECRET {q(os.environ["RUSTFS_SECRET_KEY"])},
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
)
""")

source = q("s3://hh-lake/processed/java_vacancies.parquet")

con.execute(f"""
CREATE TEMP TABLE base AS
SELECT
    vacancy_id,
    coalesce(nullif(trim(area), ''), 'Не указан') AS area,
    status,
    list_sort(list_distinct(list_filter(
        list_transform(
            string_split(coalesce(key_skills, ''), ';'),
            x -> lower(trim(regexp_replace(x, '\\s+', ' ', 'g')))
        ),
        x -> x <> ''
    ))) AS skills,
    CASE
        WHEN upper(trim(currency)) IN ('RUR', 'RUB') THEN
            CASE
                WHEN try_cast(nullif(trim(salary_from), '') AS DOUBLE) IS NOT NULL
                 AND try_cast(nullif(trim(salary_to), '') AS DOUBLE) IS NOT NULL
                THEN (
                    try_cast(nullif(trim(salary_from), '') AS DOUBLE) +
                    try_cast(nullif(trim(salary_to), '') AS DOUBLE)
                ) / 2
                WHEN try_cast(nullif(trim(salary_from), '') AS DOUBLE) IS NOT NULL
                THEN try_cast(nullif(trim(salary_from), '') AS DOUBLE)
                WHEN try_cast(nullif(trim(salary_to), '') AS DOUBLE) IS NOT NULL
                THEN try_cast(nullif(trim(salary_to), '') AS DOUBLE)
                ELSE NULL
            END
        ELSE NULL
    END AS salary
FROM read_parquet({source})
""")

con.execute("""
CREATE TEMP TABLE skill_stats AS
SELECT skill, count(*) AS vacancy_count, median(salary) AS median_salary
FROM (
    SELECT unnest(skills) AS skill, salary
    FROM base
    WHERE len(skills) > 0
)
GROUP BY skill
""")

con.execute("""
CREATE TEMP TABLE pairs AS
SELECT
    list_extract(skills, i) AS skill_a,
    list_extract(skills, j) AS skill_b,
    count(*) AS vacancy_count,
    count(salary) AS salary_count,
    median(salary) AS median_salary
FROM base,
     unnest(range(1, len(skills) + 1)) AS a(i),
     unnest(range(1, len(skills) + 1)) AS b(j)
WHERE i < j
GROUP BY skill_a, skill_b
HAVING count(salary) >= 5
""")

con.execute("""
CREATE TEMP TABLE triples AS
SELECT
    list_extract(skills, i) AS skill_a,
    list_extract(skills, j) AS skill_b,
    list_extract(skills, k) AS skill_c,
    count(*) AS vacancy_count,
    count(salary) AS salary_count,
    median(salary) AS median_salary
FROM base,
     unnest(range(1, len(skills) + 1)) AS a(i),
     unnest(range(1, len(skills) + 1)) AS b(j),
     unnest(range(1, len(skills) + 1)) AS c(k)
WHERE i < j AND j < k
GROUP BY skill_a, skill_b, skill_c
HAVING count(salary) >= 5
""")

def rows(sql):
    cur = con.execute(sql)
    names = [x[0] for x in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]

top_skills = rows("""
SELECT skill, vacancy_count, median_salary
FROM skill_stats
ORDER BY vacancy_count DESC
LIMIT 30
""")

pair_extremes = rows("""
(
    SELECT 'highest' AS kind, skill_a, skill_b, vacancy_count,
           salary_count, median_salary
    FROM pairs
    ORDER BY median_salary DESC
    LIMIT 10
)
UNION ALL
(
    SELECT 'lowest' AS kind, skill_a, skill_b, vacancy_count,
           salary_count, median_salary
    FROM pairs
    ORDER BY median_salary ASC
    LIMIT 10
)
""")

triple_extremes = rows("""
(
    SELECT 'highest' AS kind, skill_a, skill_b, skill_c, vacancy_count,
           salary_count, median_salary
    FROM triples
    ORDER BY median_salary DESC
    LIMIT 10
)
UNION ALL
(
    SELECT 'lowest' AS kind, skill_a, skill_b, skill_c, vacancy_count,
           salary_count, median_salary
    FROM triples
    ORDER BY median_salary ASC
    LIMIT 10
)
""")

regions = rows("""
SELECT
    area,
    count(*) AS vacancy_count,
    count(salary) AS salary_count,
    median(salary) AS median_salary
FROM base
GROUP BY area
ORDER BY vacancy_count DESC
LIMIT 3
""")

region_skills = {}
for region in regions:
    region_skills[region["area"]] = rows("""
        SELECT skill, count(*) AS vacancy_count
        FROM (
            SELECT area, unnest(skills) AS skill
            FROM base
            WHERE len(skills) > 0
        )
        WHERE area = ?
        GROUP BY skill
        ORDER BY vacancy_count DESC
        LIMIT 10
    """.replace("area = ?", "area = " + q(region["area"])))

result = {
    "salary_method": "RUR/RUB only; midpoint when both bounds exist, otherwise the available bound",
    "combination_minimum_salary_observations": 5,
    "overall": [
        {
            "total_rows": row[0],
            "rows_with_rub_salary": row[1],
            "median_salary": row[2]
        }
        for row in con.execute("""
            SELECT count(*) AS total_rows,
                   count(salary) AS rows_with_rub_salary,
                   median(salary) AS median_salary
            FROM base
        """).fetchall()
    ],
    "top_skills": top_skills,
    "pair_salary_extremes": pair_extremes,
    "triple_salary_extremes": triple_extremes,
    "top_three_regions": regions,
    "top_skills_by_region": region_skills,
}

out = Path("reports/salary_region_analysis.json")
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str))

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
s3.upload_file(str(out), "hh-lake", "results/salary_region_analysis.json")

print(json.dumps({
    "total_rows": result["overall"],
    "top_skills": top_skills[:10],
    "pair_salary_extremes": pair_extremes,
    "triple_salary_extremes": triple_extremes,
    "top_three_regions": regions,
    "report": str(out),
    "object": "s3://hh-lake/results/salary_region_analysis.json"
}, ensure_ascii=False, indent=2, default=str))
