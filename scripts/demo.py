#!/usr/bin/env python3
"""Run a real process-crash demonstration with isolated ports and owned cleanup."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path,
                        default=Path(__file__).resolve().parents[1] / "target/debug/ringstore")
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("build the binary first with: cargo build --locked --bin ringstore")

    processes = []
    reservations = []
    with tempfile.TemporaryDirectory(prefix="ringstore-demo-") as directory:
        config_path = Path(directory) / "config.json"
        log_path = Path(directory) / "services.log"
        try:
            for _ in range(6):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
            addresses = [f"127.0.0.1:{sock.getsockname()[1]}" for sock in reservations]
            config = {"backends": addresses[:4], "keepers": addresses[4:]}
            config_path.write_text(json.dumps(config))

            def call(*command):
                result = subprocess.run([str(binary), "--config", str(config_path), *command],
                                        capture_output=True, text=True, timeout=15, check=True)
                return [json.loads(line) for line in result.stdout.splitlines() if line]

            def start(role, index):
                port_index = index if role == "backend" else 4 + index
                if reservations[port_index] is not None:
                    reservations[port_index].close()
                    reservations[port_index] = None
                with log_path.open("a") as output:
                    process = subprocess.Popen(
                        [str(binary), "--config", str(config_path), role, "--index", str(index)],
                        stdout=output, stderr=output)
                processes.append(process)
                return process

            def eventually(description, predicate):
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if predicate():
                        return
                    time.sleep(0.05)
                raise AssertionError(f"timed out: {description}")

            backends = [start("backend", index) for index in range(4)]
            keepers = [start("keeper", index) for index in range(2)]
            eventually("all backends ready",
                       lambda: all(row["reachable"] for row in call("placement")))
            assert call("set", "status", "ready") == [True]
            assert call("append", "events", "same") == [True]
            assert call("append", "events", "same") == [True]
            assert call("list", "events") == [["same", "same"]]
            print("1. Four backends and two keepers are running.")
            print("2. Two identical appends remain two logical occurrences.")

            selected = next(row for row in call("placement") if row["write_target"])
            index = config["backends"].index(selected["address"].removeprefix("http://"))
            backends[index].kill()
            backends[index].wait()
            keepers[0].kill()
            keepers[0].wait()
            assert call("get", "status") == ["ready"]
            assert call("list", "events") == [["same", "same"]]
            print("3. Killed a replica and the primary keeper; reads still succeed.")

            backends[index] = start("backend", index)
            eventually("restarted backend ready",
                       lambda: all(row["reachable"] for row in call("placement")))

            def repaired():
                targets = {row["address"] for row in call("placement") if row["write_target"]}
                copies = call("copies", "events", "--lists")
                return all(row.get("distinct_operations") == 2
                           for row in copies if row["address"] in targets)

            eventually("backup keeper restores all current list replicas", repaired)
            assert keepers[1].poll() is None
            assert call("list", "events") == [["same", "same"]]
            assert call("remove", "events", "same") == [2]
            assert call("list", "events") == [[]]
            assert call("append", "events", "same") == [True]
            assert call("list", "events") == [["same"]]
            print("4. Backup keeper restored the empty replacement backend.")
            print("5. Removal hides old append IDs; a new equal append survives.")
            print("PASS: process crash, keeper takeover, repair, and list semantics.")
        except Exception:
            if log_path.exists():
                print(log_path.read_text()[-6000:])
            raise
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.wait()
            for reservation in reservations:
                if reservation is not None:
                    reservation.close()


if __name__ == "__main__":
    main()
