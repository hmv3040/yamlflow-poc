import ast
import json
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from yamlflow.compiler import compile_project
from yamlflow.config import load_project
from yamlflow.engine import connection, execute_node, extract


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "sales"
    shutil.copytree(Path(__file__).parents[1] / "examples/sales", root)
    return root


def run(root, db):
    _, _, nodes = load_project(root)
    for key, node in nodes.items():
        execute_node(root, db, key, node)
    return nodes


def test_end_to_end_rerun_and_source_change(project, tmp_path):
    db = tmp_path / "analytics.duckdb"
    run(project, db)
    run(project, db)
    with connection(db) as con:
        assert con.execute("select count(*) from raw.erpnext_sales_orders").fetchone()[0] == 6
        assert con.execute("select count(*), sum(order_value) from dw.fact_order").fetchone() == (4, 2800)
        assert con.execute("select sum(order_value) from analytics.analytics.daily_order_value").fetchone()[0] == 2800
    path = project / "fixtures/orders.json"
    rows = json.loads(path.read_text())
    rows[0]["base_net_total"] = 1500
    rows.pop(1)
    path.write_text(json.dumps(rows))
    run(project, db)
    with connection(db) as con:
        assert con.execute("select sum(order_value) from dw.fact_order").fetchone()[0] == 2300


def test_bad_source_preserves_previous_snapshot(project, tmp_path):
    db = tmp_path / "warehouse.duckdb"
    nodes = run(project, db)
    path = project / "fixtures/customers.json"
    rows = json.loads(path.read_text())
    rows.append(rows[0])
    path.write_text(json.dumps(rows))
    with pytest.raises(Exception, match="[Dd]uplicate"):
        execute_node(project, db, "source.erpnext.customers", nodes["source.erpnext.customers"])
    with connection(db) as con:
        assert con.execute("select count(*) from raw.erpnext_customers").fetchone()[0] == 3
        assert con.execute("select count(*) from _yamlflow.runs where status='failed'").fetchone()[0] == 1


def test_failed_quality_check_rolls_back_model(project, tmp_path):
    db = tmp_path / "warehouse.duckdb"
    nodes = run(project, db)
    node = nodes["dw.dim_customer"]
    node["sql"] = "select 'duplicate' as customer_id union all select 'duplicate'"
    with pytest.raises(ValueError, match="Quality check failed"):
        execute_node(project, db, "dw.dim_customer", node)
    with connection(db) as con:
        assert con.execute("select count(*) from dw.dim_customer").fetchone()[0] == 3


@pytest.mark.parametrize("dependency", ["dw.missing", "metrics.customer_sales"])
def test_invalid_graph_rejected(project, dependency):
    path = project / "dw/fact_order.yml"
    model = yaml.safe_load(path.read_text())
    model["depends_on"].append(dependency)
    path.write_text(yaml.safe_dump(model))
    with pytest.raises(ValueError, match="Missing dependency|Dependency cycle"):
        load_project(project)


def test_compile_is_deterministic_and_snapshots_fixtures(project, tmp_path):
    output = tmp_path / "dags"
    first = compile_project(project, output)
    source = Path(first[0]).read_text()
    ast.parse(source)
    compile_project(project, output)
    assert Path(first[0]).read_text() == source
    assert source.count("PythonOperator(") == 11
    assert "pool='duckdb_access'" in source
    assert "max_active_runs=1" in source
    snapshots = list(output.glob("_snapshot_*/manifest.json"))
    assert len(snapshots) == 1
    assert (snapshots[0].parent / "project/fixtures/orders.json").exists()


def test_frappe_pagination_and_auth(project, monkeypatch):
    _, _, nodes = load_project(project)
    monkeypatch.setenv("ERPNEXT_URL", "http://erpnext.test")
    monkeypatch.setenv("ERPNEXT_TOKEN", "key:secret")
    responses = [Mock(), Mock()]
    responses[0].json.return_value = {"data": [{"name": str(i)} for i in range(100)]}
    responses[1].json.return_value = {"data": [{"name": "last"}]}
    with patch("requests.Session") as factory:
        session = factory.return_value
        session.get.side_effect = responses
        result = extract(project, nodes["source.erpnext.customers"], "erpnext")
        assert len(result) == 101
        assert session.get.call_args.kwargs["params"]["limit_start"] == 100
        assert session.get.call_args.kwargs["headers"]["Authorization"] == "token key:secret"


def test_missing_credentials_fail_without_http(project, monkeypatch):
    _, _, nodes = load_project(project)
    monkeypatch.delenv("ERPNEXT_TOKEN", raising=False)
    with pytest.raises(ValueError, match="Set ERPNEXT_URL"):
        extract(project, nodes["source.erpnext.customers"], "erpnext")
