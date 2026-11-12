import argparse
import json
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent
HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"


def wait_for(path, message, timeout=90):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if path.exists() and message in path.read_text(encoding="utf-8"):
            return time.monotonic()
        time.sleep(0.2)

    raise TimeoutError(f"Missing log message: {message} in {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--fencing", type=int, choices=(0, 1), required=True)
    args = parser.parse_args()

    logs = ROOT / "logs"
    logs.mkdir(exist_ok=True)

    log_a = logs / f"{args.run}_coordinator-A.log"
    log_b = logs / f"{args.run}_coordinator-B.log"
    summary = logs / f"{args.run}_summary.json"
    checkpoint = ROOT / "checkpoints" / f"{args.run}.json"

    for path in (log_a, log_b, summary, checkpoint):
        if path.exists():
            raise SystemExit(f"Run already exists: {path}")

    zk = KazooClient(hosts=HOSTS, timeout=10.0)
    zk.start(timeout=15)

    base = f"/hw4/{args.run}"
    if zk.exists(base):
        zk.stop()
        zk.close()
        raise SystemExit(f"ZooKeeper run already exists: {base}")

    processes = []
    files = []
    stopped = None

    def start(name):
        output = (logs / f"{args.run}_{name}.console.log").open("w")
        files.append(output)

        command = [
            sys.executable,
            "-u",
            str(ROOT / "coordinator.py"),
            "--name",
            name,
            "--run",
            args.run,
            "--batch-delay",
            "2",
            "--fencing",
            str(args.fencing),
        ]

        process = subprocess.Popen(
            command,
            cwd=PROJECT,
            stdout=output,
            stderr=subprocess.STDOUT,
        )

        processes.append(process)
        print(f"Started {name} PID={process.pid}", flush=True)
        return process

    try:
        a = start("coordinator-A")
        wait_for(log_a, "BECAME LEADER", 30)

        b = start("coordinator-B")
        wait_for(log_b, "Ready: 13 batches", 30)
        wait_for(log_a, "READY_TO_PAUSE after checkpoint 4", 60)

        time.sleep(0.2)
        time_stopped = datetime.now(timezone.utc)
        start_pause = time.monotonic()
        a.send_signal(signal.SIGSTOP)
        stopped = a
        print(f"SIGSTOP coordinator-A at {time_stopped.isoformat()}", flush=True)

        time_leader = wait_for(log_b, "BECAME LEADER", 35)
        failover_seconds = round(time_leader - start_pause, 3)
        print(f"Failover seconds: {failover_seconds}", flush=True)

        wait_for(logs / f"{args.run}_coordinator-B.console.log", "HW3 pair frequencies: MATCH", 50)
        wait_for(log_b, "записал результат", 50)
        print("Coordinator-B completed and published the result", flush=True)

        remaining = 40 - (time.monotonic() - start_pause)
        if remaining > 0:
            time.sleep(remaining)

        pause_seconds = round(time.monotonic() - start_pause, 3)
        a.send_signal(signal.SIGCONT)
        stopped = None
        print(f"SIGCONT coordinator-A after {pause_seconds} seconds", flush=True)

        expected = (
            "BadVersionError" if args.fencing
            else "записал результат"
        )
        wait_for(log_a, expected, 35)

        data, stat = zk.get(f"{base}/result")
        pointer = data.decode()

        from worker import load_checkpoint, verify

        next_batch, counts = load_checkpoint(checkpoint)
        if next_batch != 13:
            raise AssertionError(f"Incomplete checkpoint: {next_batch}/13")

        verify(counts)

        expected_version = 1 if args.fencing else 2
        if stat.version != expected_version:
            raise AssertionError(
                f"Unexpected version: {stat.version}, expected {expected_version}"
            )

        result = {
            "run": args.run,
            "fencing": args.fencing,
            "session_timeout_seconds": 10,
            "paused_after_checkpoint": 4,
            "pause_seconds": pause_seconds,
            "failover_seconds": failover_seconds,
            "completed_batches": next_batch,
            "distinct_pairs": len(counts),
            "hw3_pair_frequencies_match": True,
            "pointer": pointer,
            "pointer_version": stat.version,
            "old_leader_publication": (
                "rejected" if args.fencing else "accepted"
            ),
        }

        summary.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("EXPERIMENT PASSED", flush=True)

    finally:
        if stopped is not None:
            stopped.send_signal(signal.SIGCONT)

        for process in processes:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)

        for process in processes:
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

        for output in files:
            output.close()

        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
