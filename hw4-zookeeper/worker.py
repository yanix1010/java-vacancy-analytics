import argparse
import itertools
import json
import os
from collections import Counter
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "input" / "java_vacancies.parquet"


def normalize_skills(value):
    if not value:
        return []
    return sorted({
        " ".join(skill.strip().split()).casefold()
        for skill in value.split(";")
        if skill.strip()
    })


def load_batches(batch_size=500):
    rows = duckdb.connect().execute(
        "SELECT key_skills FROM read_parquet(?)",
        [str(INPUT)],
    ).fetchall()

    values = [row[0] for row in rows]
    return [
        values[i:i + batch_size]
        for i in range(0, len(values), batch_size)
    ]


def count_pairs(values):
    result = Counter()

    for value in values:
        skills = normalize_skills(value)
        result.update(itertools.combinations(skills, 2))

    return result


def save_checkpoint(path, next_batch, counts):
    data = {
        "next_batch": next_batch,
        "pair_counts": [
            {"skills": list(pair), "count": count}
            for pair, count in sorted(counts.items())
        ],
    }

    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(data, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def load_checkpoint(path):
    if not path.exists():
        return 0, Counter()

    data = json.loads(path.read_text(encoding="utf-8"))
    counts = Counter({
        tuple(item["skills"]): item["count"]
        for item in data["pair_counts"]
    })

    return data["next_batch"], counts


def calculate(checkpoint, batch_size=500):
    batches = load_batches(batch_size)
    next_batch, counts = load_checkpoint(checkpoint)

    if not 0 <= next_batch <= len(batches):
        raise ValueError("Invalid checkpoint position")

    print(f"Loaded checkpoint: {next_batch}/{len(batches)}", flush=True)

    for index in range(next_batch, len(batches)):
        counts.update(count_pairs(batches[index]))
        save_checkpoint(checkpoint, index + 1, counts)
        print(
            f"Checkpoint saved: {index + 1}/{len(batches)}",
            flush=True,
        )

    return counts


def verify(counts):
    baseline = json.loads(
        (ROOT.parent / "results" / "mapreduce_s3_range_64mib.json")
        .read_text(encoding="utf-8")
    )

    expected = {
        tuple(item["skills"]): item["count"]
        for item in baseline["pair_counts"]
    }

    if dict(counts) != expected:
        raise AssertionError("Pair frequencies differ from HW3")

    print(f"Distinct pairs: {len(counts)}", flush=True)
    print("HW3 pair frequencies: MATCH", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=ROOT / "checkpoints" / "worker_test.json",
    )
    args = parser.parse_args()

    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    counts = calculate(args.checkpoint)
    verify(counts)


if __name__ == "__main__":
    main()
