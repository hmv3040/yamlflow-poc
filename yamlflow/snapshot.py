"""Publish a complete, atomic analytics snapshot without exposing warehouse writers."""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .engine import connection, table


def publish_snapshot(db_path, output):
    with connection(db_path) as con:
        catalog = con.execute("SELECT current_database()").fetchone()[0]
        relations = con.execute("""SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema IN ('dw', 'analytics') AND table_catalog = ?
            ORDER BY table_schema, table_name""", [catalog]).fetchall()
        if not relations:
            raise ValueError("No curated tables yet. Run the pipeline first.")
        tables = {}
        for schema, name in relations:
            qualified = table(f"{schema}.{name}", catalog)
            columns = [{"name": r[0], "type": r[1]} for r in con.execute(f"DESCRIBE {qualified}").fetchall()]
            rows = con.execute(f"SELECT * FROM {qualified} LIMIT 100001").fetchall()
            if len(rows) > 100000:
                raise ValueError(f"{schema}.{name} exceeds the POC snapshot limit (100,000 rows). No snapshot was replaced.")
            tables[f"{schema}.{name}"] = {"columns": columns, "rows": rows}
        snapshot = {"version": 1, "published_at": datetime.now(timezone.utc).isoformat(), "tables": tables}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_text(json.dumps(snapshot, default=str))
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return {"snapshot": str(output), "tables": len(tables), "published_at": snapshot["published_at"]}


def publish_manifest_snapshot(manifest_path):
    from .engine import resolve_db

    manifest = json.loads(Path(manifest_path).read_text())
    root = Path(manifest_path).parent / "project"
    db = resolve_db(root, manifest["project"]["warehouse"]["path"])
    return publish_snapshot(db, db.parent / "analytics_snapshot.json")
