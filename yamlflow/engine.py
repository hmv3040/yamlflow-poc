"""Runtime shared by the local CLI and generated Airflow tasks."""
import contextlib
import fcntl
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote



def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def table(name, catalog=None):
    parts = ([catalog] if catalog else []) + name.split(".")
    return ".".join(quoted(part) for part in parts)


def resolve_db(root, configured):
    return Path(os.environ.get("YAMLFLOW_DB", str(Path(root) / configured))).resolve()


@contextlib.contextmanager
def connection(path):
    import duckdb

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Pool handles Airflow scheduling; flock also coordinates CLI and DAG runs.
    with open(str(path) + ".lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        con = duckdb.connect(str(path))
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS _yamlflow")
            con.execute("""CREATE TABLE IF NOT EXISTS _yamlflow.runs
                (run_id VARCHAR, node VARCHAR, status VARCHAR, rows BIGINT,
                 started_at TIMESTAMP, finished_at TIMESTAMP, error VARCHAR)""")
            yield con
        finally:
            con.close()
            fcntl.flock(lock, fcntl.LOCK_UN)


def extract(root, node, mode):
    resource = node["resource"]
    if mode == "fixture":
        return json.loads((Path(root) / resource["fixture"]).read_text())
    if mode != "erpnext":
        raise ValueError(f"Unknown source mode: {mode}")
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    source = node["source"]
    url = os.environ.get(source["base_url_env"], "").rstrip("/")
    token = os.environ.get(source["token_env"], "")
    if not url or not token:
        raise ValueError(f"Set {source['base_url_env']} and {source['token_env']}")
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
    records, offset = [], 0
    with session:
        while True:
            response = session.get(
                url + "/api/resource/" + quote(resource["doctype"], safe=""),
                headers={"Authorization": "token " + token},
                params={"fields": json.dumps(list(resource["columns"])),
                        "limit_start": offset, "limit_page_length": 100,
                        "order_by": "name asc"}, timeout=60)
            response.raise_for_status()
            batch = response.json()["data"]
            records.extend(batch)
            if len(batch) < 100:
                return records
            offset += len(batch)


def check(con, node):
    catalog = con.execute("SELECT current_database()").fetchone()[0]
    for test in node["tests"]:
        col = quoted(test["column"])
        relation = table(node["target"], catalog)
        if test["kind"] == "not_null":
            count = con.execute(f"SELECT count(*) FROM {relation} WHERE {col} IS NULL").fetchone()[0]
        else:
            count = con.execute(f"SELECT count(*) FROM (SELECT {col} FROM {relation} GROUP BY {col} HAVING count(*) > 1)").fetchone()[0]
        if count:
            raise ValueError(f"Quality check failed: {node['target']}.{test['column']} {test['kind']} ({count})")


def metric_sql(node, catalog=None):
    expressions = [quoted(dim) for dim in node["dimensions"]]
    for measure in node["measures"]:
        arg = quoted(measure["column"]) if measure["column"] else "*"
        expressions.append(f"{measure['aggregation']}({arg}) AS {quoted(measure['name'])}")
    group = " GROUP BY " + ", ".join(quoted(d) for d in node["dimensions"]) if node["dimensions"] else ""
    return f"SELECT {', '.join(expressions)} FROM {table(node['model'], catalog)}{group}"


def execute_node(root, db_path, key, node, mode="fixture", action="build"):
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    # Fetch fully before replacing the previous successful snapshot.
    records = extract(root, node, mode) if node["kind"] == "source" and action == "build" else None
    with connection(db_path) as con:
        catalog = con.execute("SELECT current_database()").fetchone()[0]
        try:
            con.execute("BEGIN")
            if action == "test":
                check(con, node)
            else:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted(node['target'].split('.')[0])}")
                target = table(node["target"], catalog)
                if node["kind"] == "source":
                    resource = node["resource"]
                    columns = resource["columns"]
                    definition = ", ".join(f"{quoted(k)} {v}" for k, v in columns.items())
                    pk = ", ".join(quoted(k) for k in resource["primary_key"])
                    con.execute(f"CREATE OR REPLACE TABLE {target} ({definition}, PRIMARY KEY ({pk}))")
                    if records:
                        placeholders = ", ".join("?" for _ in columns)
                        con.executemany(f"INSERT INTO {target} VALUES ({placeholders})", [[row.get(k) for k in columns] for row in records])
                else:
                    sql = node["sql"] if node["kind"] == "dw" else metric_sql(node, catalog)
                    con.execute(f"CREATE OR REPLACE TABLE {target} AS {sql}")
                    # Validate before commit: failed quality checks preserve the last good model.
                    check(con, node)
            count = con.execute(f"SELECT count(*) FROM {table(node['target'], catalog)}").fetchone()[0]
            con.execute("COMMIT")
        except Exception as exc:
            con.execute("ROLLBACK")
            con.execute("INSERT INTO _yamlflow.runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [str(uuid.uuid4()), key + ":" + action, "failed", None, started, datetime.now(timezone.utc).replace(tzinfo=None), type(exc).__name__])
            raise
        con.execute("INSERT INTO _yamlflow.runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [str(uuid.uuid4()), key + ":" + action, "success", count, started, datetime.now(timezone.utc).replace(tzinfo=None), None])
        return {"node": key, "action": action, "rows": count}


def run_manifest_task(manifest_path, node_id, action="build"):
    manifest = json.loads(Path(manifest_path).read_text())
    root = Path(manifest_path).parent / "project"
    configured = manifest["project"]["warehouse"]["path"]
    return execute_node(root, resolve_db(root, configured), node_id, manifest["nodes"][node_id],
                        os.environ.get("YAMLFLOW_SOURCE_MODE", "fixture"), action)
