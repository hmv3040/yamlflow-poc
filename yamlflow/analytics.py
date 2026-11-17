"""A bounded SQL tool over an isolated, in-memory copy of curated snapshot data."""
import json
import re
import threading
from pathlib import Path

import duckdb
import sqlglot
from sqlglot import exp

SAFE_FUNCTIONS = {
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND", "ABS", "COALESCE", "NULLIF",
    "CAST", "TRY_CAST", "DATE_TRUNC", "TIMESTAMP_TRUNC", "EXTRACT", "LOWER",
    "UPPER", "LENGTH", "CONCAT", "SUBSTRING", "TRIM", "ROW_NUMBER", "RANK",
    "DENSE_RANK", "LAG", "LEAD", "IF", "CASE", "DATE_DIFF", "DATEDIFF",
    "PERCENT_RANK", "CUME_DIST", "NTILE", "STRFTIME", "TIME_TO_STR",
}

EXAMPLES = {
    "What is the total submitted order value?": "SELECT count(*) AS order_count, sum(order_value) AS order_value FROM dw.fact_order",
    "Which customers have the highest order value?": "SELECT customer_name, order_count, order_value FROM analytics.customer_sales ORDER BY order_value DESC",
    "How does order value change by day?": "SELECT order_date, order_count, order_value, average_order_value FROM analytics.daily_order_value ORDER BY order_date",
    "What percentage of order value comes from each customer?": "SELECT customer_name, order_value, round(100.0 * order_value / nullif(sum(order_value) OVER (), 0), 2) AS share_percent FROM analytics.customer_sales ORDER BY order_value DESC",
}


def load_snapshot(path):
    payload = json.loads(Path(path).read_text())
    if payload.get("version") != 1 or not payload.get("tables"):
        raise ValueError("Invalid or empty analytics snapshot")
    return payload


def safe_identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid snapshot identifier")
    return '"' + value + '"'


class AnalyticsWarehouse:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.con = duckdb.connect(":memory:", config={
            "enable_external_access": False, "memory_limit": "128MB", "threads": 1,
        })
        try:
            for name, data in snapshot["tables"].items():
                schema, relation = name.split(".")
                qualified = safe_identifier(schema) + "." + safe_identifier(relation)
                self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {safe_identifier(schema)}")
                definitions = []
                for column in data["columns"]:
                    kind = column["type"]
                    if not re.fullmatch(r"VARCHAR|INTEGER|BIGINT|HUGEINT|DOUBLE|FLOAT|BOOLEAN|DATE|TIMESTAMP(?:_NS| WITH TIME ZONE)?|DECIMAL\(\d{1,2},\d{1,2}\)", kind):
                        raise ValueError(f"Unsupported snapshot type: {kind}")
                    definitions.append(f"{safe_identifier(column['name'])} {kind}")
                self.con.execute(f"CREATE TABLE {qualified} ({', '.join(definitions)})")
                if data["rows"]:
                    values = ",".join("?" for _ in definitions)
                    self.con.executemany(f"INSERT INTO {qualified} VALUES ({values})", data["rows"])
            self.con.execute("SET lock_configuration = true")
        except Exception:
            self.con.close()
            raise

    def close(self):
        self.con.close()

    def schema(self):
        return {name: {"columns": data["columns"], "row_count": len(data["rows"])}
                for name, data in self.snapshot["tables"].items()}

    def validate(self, sql):
        if len(sql) > 12000:
            raise ValueError("Query is too long")
        statements = sqlglot.parse(sql, read="duckdb")
        if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.Union)):
            raise ValueError("Only one SELECT query is allowed")
        tree = statements[0]
        if any(isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Command, exp.Into)) for node in tree.walk()):
            raise ValueError("Only read-only SELECT queries are allowed")
        if any(node.args.get("recursive") for node in tree.find_all(exp.With)):
            raise ValueError("Recursive queries are not supported")
        ctes = {node.alias_or_name for node in tree.find_all(exp.CTE)}
        for node in tree.find_all(exp.Table):
            if not isinstance(node.this, exp.Identifier) or node.catalog:
                raise ValueError("File, function, and catalog table references are not allowed")
            if not node.db and node.name in ctes:
                continue
            if f"{node.db}.{node.name}" not in self.snapshot["tables"]:
                raise ValueError(f"Use an approved schema-qualified table: {node.sql()}")
        for node in tree.find_all(exp.Func):
            name = node.name.upper() if isinstance(node, exp.Anonymous) else node.sql_name()
            if name not in SAFE_FUNCTIONS:
                raise ValueError(f"Function is not enabled: {name}")
        return tree.sql(dialect="duckdb")

    def query(self, sql, limit=100):
        sql = self.validate(sql)
        timer = threading.Timer(4, self.con.interrupt)
        timer.start()
        try:
            result = self.con.execute(f"SELECT * FROM ({sql}) AS _result LIMIT {limit + 1}")
            columns = [item[0] for item in result.description]
            if len(columns) != len(set(columns)):
                raise ValueError("Give duplicate result columns unique aliases")
            rows = result.fetchall()
            return {"sql": sql, "columns": columns,
                    "rows": [dict(zip(columns, row)) for row in rows[:limit]],
                    "truncated": len(rows) > limit, "published_at": self.snapshot["published_at"]}
        finally:
            timer.cancel()
            timer.join()
