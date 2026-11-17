"""Run inside the Airflow container: python - < scripts/verify_airflow.py."""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "http://localhost:8080"
session = requests.Session()
deadline = time.monotonic() + 1200


def request(method, path, **kwargs):
    response = session.request(method, BASE + path, timeout=120, **kwargs)
    if not response.ok:
        raise RuntimeError(f"{method} {path}: {response.status_code}: {response.text[:1000]}")
    return response.json()


print("Waiting for Airflow API and generated DAG...", flush=True)
while time.monotonic() < deadline:
    try:
        passwords = json.loads(Path("/opt/airflow/simple_auth_manager_passwords.json.generated").read_text())
        token = request("POST", "/auth/token", json={"username": "admin", "password": passwords["admin"]})
        session.headers["Authorization"] = "Bearer " + token["access_token"]
        response = session.get(BASE + "/api/v2/dags/sales_analytics", timeout=10)
        if response.ok:
            break
    except (OSError, requests.RequestException, RuntimeError):
        pass
    time.sleep(5)
else:
    raise SystemExit("Timed out waiting for Airflow")

errors = request("GET", "/api/v2/importErrors")
if errors["total_entries"]:
    raise SystemExit("Airflow has DAG import errors: " + json.dumps(errors))

request("PATCH", "/api/v2/dags/sales_analytics", json={"is_paused": False})
run_id = "poc_verify_" + uuid.uuid4().hex[:12]
run = request("POST", "/api/v2/dags/sales_analytics/dagRuns", json={
    "dag_run_id": run_id,
    "logical_date": datetime.now(timezone.utc).isoformat(),
    "conf": {},
})
print("Triggered " + run_id, flush=True)
previous = None
while time.monotonic() < deadline:
    run = request("GET", "/api/v2/dags/sales_analytics/dagRuns/" + run_id)
    if run["state"] != previous:
        print("DAG state: " + run["state"], flush=True)
        previous = run["state"]
    if run["state"] == "success":
        tasks = request("GET", "/api/v2/dags/sales_analytics/dagRuns/" + run_id + "/taskInstances")
        states = {task["task_id"]: task["state"] for task in tasks["task_instances"]}
        print(json.dumps({"run_id": run_id, "tasks": states}, indent=2), flush=True)
        if len(states) != 11 or any(state != "success" for state in states.values()):
            raise SystemExit("Unexpected task results")
        break
    if run["state"] == "failed":
        raise SystemExit("DAG failed; inspect its task logs in Airflow")
    time.sleep(5)
else:
    raise SystemExit("Timed out waiting for DAG completion")
