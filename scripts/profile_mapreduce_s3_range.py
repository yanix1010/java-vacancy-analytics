import json
import os
import sys
import time
import subprocess
import threading
from pathlib import Path

import psutil
import boto3
from botocore.config import Config

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

runs = []

for attempt in range(1, 4):
    process = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "scripts/mapreduce_s3_range.py",
            "--sizes",
            "64",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    proc = psutil.Process(process.pid)
    peak_rss = 0
    started = time.perf_counter()
    output_lines = []

    def collect_output():
        for line in process.stdout:
            output_lines.append(line)

    reader = threading.Thread(target=collect_output, daemon=True)
    reader.start()

    while process.poll() is None:
        try:
            current = proc.memory_info().rss
            for child in proc.children(recursive=True):
                try:
                    current += child.memory_info().rss
                except psutil.Error:
                    pass
            peak_rss = max(peak_rss, current)
        except psutil.Error:
            pass
        time.sleep(0.01)

    reader.join()
    elapsed = time.perf_counter() - started
    output = "".join(output_lines)

    if process.returncode != 0:
        print(output)
        raise SystemExit(
            f"Attempt {attempt} failed with exit code {process.returncode}"
        )

    result_lines = [
        line for line in output.splitlines()
        if line.startswith("{") and "chunk_size_mib" in line
    ]
    if not result_lines:
        print(output)
        raise SystemExit(f"No result JSON found in attempt {attempt}")

    result = json.loads(result_lines[-1])
    run = {
        "attempt": attempt,
        "chunk_size_mib": 64,
        "elapsed_seconds": round(elapsed, 3),
        "peak_rss_mib": round(peak_rss / (1024 * 1024), 2),
        "exit_code": process.returncode,
        "source_rows": result["source_rows"],
        "java_vacancies": result["java_vacancies"],
        "distinct_skills": result["distinct_skills"],
        "distinct_pairs": result["distinct_pairs"],
        "distinct_triples": result["distinct_triples"],
    }
    runs.append(run)
    print(json.dumps(run, ensure_ascii=False), flush=True)

report = {
    "profiled_program": "scripts/mapreduce_s3_range.py",
    "chunk_size_mib": 64,
    "attempts": runs,
    "average_elapsed_seconds": round(
        sum(item["elapsed_seconds"] for item in runs) / len(runs), 3
    ),
    "maximum_observed_peak_rss_mib": max(
        item["peak_rss_mib"] for item in runs
    ),
    "measurement_note": (
        "Elapsed time includes process startup and S3 processing. "
        "RSS sampled every 10 ms; peak is the maximum observed process-tree RSS."
    ),
}

path = Path("reports/mapreduce_s3_range_profile.json")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(report, ensure_ascii=False, indent=2))

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
s3.upload_file(
    str(path),
    "hh-lake",
    "results/mapreduce_s3_range_profile.json",
)

print("Profile report:", path)
print("RustFS object: s3://hh-lake/results/mapreduce_s3_range_profile.json")
print(json.dumps(report, ensure_ascii=False, indent=2))
