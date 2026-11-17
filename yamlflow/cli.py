import argparse
import json
import sys

from .compiler import compile_project
from .config import load_project
from .engine import connection, execute_node, resolve_db, table


def main():
    parser = argparse.ArgumentParser(description="YAML-driven analytics POC")
    parser.add_argument("command", choices=["validate", "plan", "compile", "run", "test", "report", "publish"])
    parser.add_argument("--project", default="examples/sales")
    parser.add_argument("--output", default="generated/dags")
    parser.add_argument("--mode", choices=["fixture", "erpnext"], default="fixture")
    args = parser.parse_args()
    try:
        root, project, nodes = load_project(args.project)
        db = resolve_db(root, project.warehouse.path)
        if args.command == "validate":
            print(f"Valid: {len(nodes)} nodes, {len(project.pipelines)} pipeline(s)")
        elif args.command == "plan":
            for key, node in nodes.items():
                print(f"{key} -> {node['target']} | after: {', '.join(node['depends_on']) or '(start)'}")
        elif args.command == "compile":
            for path in compile_project(root, args.output):
                print(path)
        elif args.command in ("run", "test"):
            for key, node in nodes.items():
                if args.command == "test" and not node["tests"]:
                    continue
                result = execute_node(root, db, key, node, args.mode,
                                      "test" if args.command == "test" else "build")
                print(json.dumps(result))
        elif args.command == "publish":
            from .snapshot import publish_snapshot
            print(json.dumps(publish_snapshot(db, db.parent / "analytics_snapshot.json")))
        else:
            with connection(db) as con:
                catalog = con.execute("SELECT current_database()").fetchone()[0]
                for node in nodes.values():
                    if node["kind"] == "metrics":
                        print(node["target"])
                        result = con.execute("SELECT * FROM " + table(node["target"], catalog) + " ORDER BY 1")
                        print(json.dumps([dict(zip([d[0] for d in result.description], row)) for row in result.fetchall()], default=str, indent=2))
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
