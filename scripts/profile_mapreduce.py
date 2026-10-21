import json
import time
import subprocess
import threading
from pathlib import Path

import psutil
import boto3
import os
from botocore.config import Config

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))

runs = []

for attempt in range(1, 4):
    process = subprocess.Popen(
        ["python", "-u", "scripts/mapreduce_skills.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    proc = psutil.Process(process.pid)
    peak_rss = 0
    started = time.perf_counter()

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

    output, _ = process.communicate()
    elapsed = time.perf_counter() - started

    if process.returncode != 0:
        print(output)
        raise SystemExit(f"Attempt {attempt} failed with exit code {process.returncode}")

    run = {
        "attempt": attempt,
        "elapsed_seconds": round(elapsed, 3),
        "peak_rss_mib": round(peak_rss / (1024 * 1024), 2),
        "exit_code": process.returncode,
    }
    runs.append(run)
    print(json.dumps(run, ensure_ascii=False), flush=True)

report = {
    "profiled_program": "scripts/mapreduce_skills.py",
    "attempts": runs,
    "average_elapsed_seconds": round(
        sum(x["elapsed_seconds"] for x in runs) / len(runs), 3
    ),
    "maximum_observed_peak_rss_mib": max(x["peak_rss_mib"] for x in runs),
    "measurement_note": "RSS sampled every 10 ms; peak is the maximum observed process RSS.",
}

path = Path("reports/mapreduce_profile.json")
path.parent.mkdir(exist_ok=True)
path.write_text(json.dumps(report, ensure_ascii=False, indent=2))

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
s3.upload_file(str(path), "hh-lake", "results/mapreduce_profile.json")

print("Profile report:", path)
print("RustFS object: s3://hh-lake/results/mapreduce_profile.json")
print(json.dumps(report, ensure_ascii=False, indent=2))
