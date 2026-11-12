import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import BadVersionError, NodeExistsError, ConnectionLoss, SessionExpiredError

from worker import (
    count_pairs,
    load_batches,
    load_checkpoint,
    save_checkpoint,
    verify,
)

ROOT = Path(__file__).resolve().parent
HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--batch-delay", type=float, default=5)
    parser.add_argument("--fencing", type=int, choices=[0, 1], required=True)
    args = parser.parse_args()

    if args.batch_delay < 0:
        parser.error("batch-delay must be nonnegative")

    logs = ROOT / "logs"
    logs.mkdir(exist_ok=True)
    log_path = logs / f"{args.run}_{args.name}.log"

    def log(message):
        line = f"{timestamp()} [{args.name}] {message}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as output:
            output.write(line + "\n")

    base = f"/hw4/{args.run}"
    epoch_path = f"{base}/epoch"
    pointer_path = f"{base}/result"
    checkpoint = ROOT / "checkpoints" / f"{args.run}.json"

    zk = KazooClient(hosts=HOSTS, timeout=10.0)

    def state_changed(state):
        log(f"ZooKeeper state: {state}")

    zk.add_listener(state_changed)
    zk.start(timeout=15)
    zk.ensure_path(base)

    for path, initial in (
        (epoch_path, b"0"),
        (pointer_path, b"initial"),
    ):
        try:
            zk.create(path, initial)
        except NodeExistsError:
            pass

    lock = zk.Lock(f"{base}/leader", identifier=args.name)
    batches = load_batches()
    log(f"PID={os.getpid()} FENCING={args.fencing}")
    log(f"Ready: {len(batches)} batches")

    def wait_connected():
        while zk.state != KazooState.CONNECTED:
            time.sleep(0.5)

    def read_epoch():
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            if zk.state != KazooState.CONNECTED:
                time.sleep(0.25)
                continue

            try:
                value, _ = zk.get(epoch_path)
                return int(value.decode())
            except (ConnectionLoss, SessionExpiredError):
                log("Waiting for ZooKeeper reconnection")
                time.sleep(0.25)

        raise TimeoutError("ZooKeeper reconnection timeout")

    def increment_epoch():
        while True:
            value, stat = zk.get(epoch_path)
            epoch = int(value.decode()) + 1
            try:
                zk.set(
                    epoch_path,
                    str(epoch).encode(),
                    version=stat.version,
                )
                return epoch
            except BadVersionError:
                continue

    def publish(epoch, epoch_version):
        payload = (
            f"coordinator={args.name};epoch={epoch};"
            f"pairs=15755"
        ).encode()

        try:
            if args.fencing:
                transaction = zk.transaction()
                transaction.check(epoch_path, version=epoch_version)
                transaction.set_data(pointer_path, payload)
                results = transaction.commit()

                for result in results:
                    if isinstance(result, Exception):
                        raise result

                _, stat = zk.get(pointer_path)
            else:
                stat = zk.set(pointer_path, payload)

            log(
                f"записал результат epoch={epoch} "
                f"pointer_version={stat.version}"
            )
        except BadVersionError:
            log(
                f"BadVersionError: stale publication rejected "
                f"epoch={epoch} epoch_version={epoch_version}"
            )

    try:
        while True:
            wait_connected()

            if not lock.acquire(blocking=False):
                time.sleep(0.5)
                continue

            epoch = increment_epoch()
            _, pointer_stat = zk.get(pointer_path)
            epoch_version = zk.get(epoch_path)[1].version

            log(
                f"BECAME LEADER epoch={epoch} "
                f"epoch_version={epoch_version}"
            )

            next_batch, counts = load_checkpoint(checkpoint)
            log(f"Loaded checkpoint: {next_batch}/{len(batches)}")

            stale = False

            for index in range(next_batch, len(batches)):
                wait_connected()

                current_epoch = read_epoch()
                if current_epoch != epoch:
                    stale = True
                    break

                counts.update(count_pairs(batches[index]))
                time.sleep(args.batch_delay)
                wait_connected()

                current_epoch = read_epoch()
                if current_epoch != epoch:
                    stale = True
                    break

                save_checkpoint(checkpoint, index + 1, counts)
                log(f"Checkpoint saved: {index + 1}/{len(batches)}")

                if index == 3:
                    log("READY_TO_PAUSE after checkpoint 4")

            if stale:
                log(f"STALE LEADER epoch={epoch}")
                next_batch, counts = load_checkpoint(checkpoint)
                log(
                    f"Recovered checkpoint: "
                    f"{next_batch}/{len(batches)}"
                )
                if next_batch != len(batches):
                    log("Waiting for completed checkpoint")
                    while next_batch != len(batches):
                        time.sleep(0.5)
                        next_batch, counts = load_checkpoint(checkpoint)

                verify(counts)
                publish(epoch, epoch_version)
                break

            verify(counts)
            log(f"CALCULATION COMPLETE epoch={epoch}")
            publish(epoch, epoch_version)
            log("Waiting for experiment completion")

            while True:
                time.sleep(1)
                wait_connected()
                current_epoch = read_epoch()

                if current_epoch != epoch:
                    log(f"STALE LEADER epoch={epoch}")
                    next_batch, counts = load_checkpoint(checkpoint)
                    if next_batch == len(batches):
                        verify(counts)
                        publish(epoch, epoch_version)
                    break

            break

    except KeyboardInterrupt:
        log("Stopped by user")
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
