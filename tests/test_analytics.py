import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from yamlflow.analytics import AnalyticsWarehouse, EXAMPLES, load_snapshot
from yamlflow.agent import answer_question
from yamlflow.config import load_project
from yamlflow.engine import execute_node
from yamlflow.snapshot import publish_snapshot


@pytest.fixture
def snapshot_file(tmp_path):
    project = tmp_path / "project"
    shutil.copytree(Path(__file__).parents[1] / "examples/sales", project)
    root, _, nodes = load_project(project)
    db = tmp_path / "analytics.duckdb"
    for key, node in nodes.items():
        execute_node(root, db, key, node)
    output = tmp_path / "analytics_snapshot.json"
    publish_snapshot(db, output)
    return output


@pytest.fixture
def warehouse(snapshot_file):
    instance = AnalyticsWarehouse(load_snapshot(snapshot_file))
    yield instance
    instance.close()


def test_snapshot_and_demo_questions(warehouse, snapshot_file):
    assert set(warehouse.schema()) == {"dw.dim_customer", "dw.fact_order", "analytics.customer_sales", "analytics.daily_order_value"}
    result = warehouse.query(next(iter(EXAMPLES.values())))
    assert result["rows"][0]["order_value"] == 2800
    assert result["rows"][0]["order_count"] == 4
    for sql in EXAMPLES.values():
        assert warehouse.query(sql)["rows"]
    assert "raw.erpnext" not in snapshot_file.read_text()


@pytest.mark.parametrize("sql", [
    "DROP TABLE dw.fact_order", "SELECT 1; SELECT 2",
    "SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM '/etc/passwd'",
    "SELECT * FROM information_schema.tables", "SELECT getenv('OPENAI_API_KEY')",
    "SELECT * FROM dw.fact_order; ATTACH '/tmp/other.db' AS other",
    "WITH x AS (SELECT * FROM read_json('/tmp/private.json')) SELECT * FROM x",
    "SELECT * FROM duckdb_secrets()", "SELECT * INTO stolen FROM dw.fact_order",
    "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT * FROM x) SELECT * FROM x",
])
def test_rejects_unsafe_or_out_of_scope_sql(warehouse, sql):
    with pytest.raises(Exception):
        warehouse.query(sql)
    assert warehouse.query("SELECT count(*) AS n FROM dw.fact_order")["rows"][0]["n"] == 4


def test_ctes_are_allowed_and_output_is_bounded(warehouse):
    result = warehouse.query("WITH orders AS (SELECT * FROM dw.fact_order) SELECT * FROM orders", limit=2)
    assert len(result["rows"]) == 2
    assert result["truncated"]


def test_agent_repairs_sql_and_uses_tool_evidence(warehouse):
    def call(name, arguments, call_id):
        return SimpleNamespace(type="function_call", name=name, arguments=json.dumps(arguments), call_id=call_id)
    def response(*calls, text=""):
        return SimpleNamespace(output=list(calls), output_text=text)
    client = Mock()
    client.responses.create.side_effect = [
        response(call("get_schema", {}, "1")),
        response(call("run_sql", {"sql": "DELETE FROM dw.fact_order"}, "2")),
        response(call("run_sql", {"sql": "SELECT sum(order_value) AS total FROM dw.fact_order"}, "3")),
        response(text="Submitted order value is 2,800, from dw.fact_order."),
    ]
    answer = answer_question("Total order value?", warehouse, client, "configured-model")
    assert answer["results"][0]["rows"][0]["total"] == 2800
    assert [step["status"] for step in answer["trace"]] == ["success", "error", "success"]
    assert all(call.kwargs["store"] is False for call in client.responses.create.call_args_list)


def test_agent_does_not_present_unverified_answer(warehouse):
    client = Mock()
    client.responses.create.return_value = SimpleNamespace(output=[], output_text="Revenue is 9 million.")
    answer = answer_question("Revenue?", warehouse, client, "configured-model")
    assert answer["answer"].startswith("No verified query result")
    assert not answer["results"]
    assert "9 million" not in answer["answer"]


def test_streamlit_demo_and_query_guard(snapshot_file, monkeypatch):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("YAMLFLOW_SNAPSHOT", str(snapshot_file))
    app = AppTest.from_file(str(Path(__file__).parents[1] / "streamlit_app.py")).run(timeout=30)
    assert not app.exception
    assert app.metric[0].value == "2,800.00"
    next(button for button in app.button if button.label == next(iter(EXAMPLES))).click().run(timeout=30)
    assert not app.exception
    assert len(app.chat_message) == 2
    app.text_area[0].set_value("DROP TABLE dw.fact_order")
    next(button for button in app.button if button.label == "Run query").click().run(timeout=30)
    assert not app.exception
    assert any("SELECT" in message.value for message in app.error)
    app.radio[0].set_value("AI agent").run(timeout=30)
    next(button for button in app.button if button.label == next(iter(EXAMPLES))).click().run(timeout=30)
    assert not app.exception
    assert any("API key" in warning.value for warning in app.warning)
