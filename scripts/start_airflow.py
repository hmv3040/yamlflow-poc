"""Supervise the three Airflow processes this non-deferrable local POC needs."""
import os
import signal
import subprocess
import time

children = []
stopping = False


def stop(signum, frame):
    global stopping
    stopping = True


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

def initialize(command):
    for attempt in range(3):
        result = subprocess.run(["airflow", *command])
        if result.returncode == 0:
            return
        if stopping:
            raise SystemExit(0)
        print(f"Initialization attempt {attempt + 1} failed; retrying...", flush=True)
        time.sleep(5)
    raise RuntimeError(f"Airflow initialization failed: {command}")


initialize(["db", "migrate"])
initialize(["pools", "set", "duckdb_access", "1", "Serialize DuckDB access"])

try:
    for command in (["api-server", "--workers", "1"], ["dag-processor"], ["scheduler"]):
        children.append(subprocess.Popen(["airflow", *command], start_new_session=True))
    while not stopping:
        exited = [child for child in children if child.poll() is not None]
        if exited:
            raise RuntimeError(f"Airflow subprocess exited: {exited[0].args} ({exited[0].returncode})")
        time.sleep(1)
finally:
    for child in children:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
    deadline = time.monotonic() + 20
    while any(child.poll() is None for child in children) and time.monotonic() < deadline:
        time.sleep(0.2)
    for child in children:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
