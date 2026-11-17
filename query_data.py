import duckdb

with duckdb.connect("data/analytics.duckdb", read_only=True) as con:
    con.sql("SELECT * FROM dw.fact_order").show()

    con.sql("""
        SELECT *
        FROM analytics.analytics.daily_order_value
        ORDER BY order_date
    """).show()