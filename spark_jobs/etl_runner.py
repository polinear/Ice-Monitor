"""Periodic ETL runner that keeps Hive and Superset demo data fresh."""
import os
import socket
import subprocess
import sys
import time


REQUIRED_SERVICES = [
    ("namenode", 9000),
    ("hive-metastore", 9083),
    ("hive-server", 10000),
    ("spark-master", 7077),
]

ETL_INTERVAL_SEC = int(os.environ.get("ETL_INTERVAL_SEC", "90"))
ETL_MASTER = os.environ.get("ETL_MASTER", "local[*]")
ETL_JOB = os.environ.get("ETL_JOB", "/jobs/etl_to_hive.py")


def wait_for_service(host: str, port: int, retries: int = 90, delay: int = 2):
    for attempt in range(1, retries + 1):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"{host}:{port} is ready", flush=True)
                return
        except OSError as exc:
            print(
                f"Waiting for {host}:{port} ({attempt}/{retries}): {exc}",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"{host}:{port} did not become ready in time")


def run_etl_cycle():
    cmd = [sys.executable, ETL_JOB]
    env = os.environ.copy()
    env.setdefault("SPARK_MASTER", ETL_MASTER)
    print(f"Starting ETL cycle with master={ETL_MASTER}", flush=True)
    return subprocess.run(cmd, env=env, check=False).returncode


def main():
    for host, port in REQUIRED_SERVICES:
        wait_for_service(host, port)

    while True:
        rc = run_etl_cycle()
        if rc == 0:
            print("ETL cycle finished successfully", flush=True)
        else:
            print(f"ETL cycle failed with code {rc}", flush=True)
        time.sleep(ETL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
