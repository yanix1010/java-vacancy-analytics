import argparse
import time
from datetime import datetime, timezone

from kazoo.client import KazooClient, KazooState


def log(name, message):
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    print(f"{timestamp} [{name}] {message}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    zk = KazooClient(
        hosts="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",
        timeout=10.0,
    )

    state = {"value": None}

    def on_state_change(new_state):
        state["value"] = new_state
        log(args.name, f"ZooKeeper state: {new_state}")

    zk.add_listener(on_state_change)
    zk.start(timeout=15)
    zk.ensure_path("/hw4")

    lock = zk.Lock("/hw4/leader", identifier=args.name)
    leader = False

    try:
        while True:
            if state["value"] != KazooState.CONNECTED:
                leader = False
                time.sleep(0.5)
                continue

            if not leader:
                if lock.acquire(blocking=False):
                    leader = True
                    log(args.name, "BECAME LEADER")
                else:
                    log(args.name, "Waiting for leadership")
                    time.sleep(2)
                    continue

            time.sleep(1)
    except KeyboardInterrupt:
        log(args.name, "Stopping")
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
