"""Local agentic analytics UI. Run with `make analytics`."""
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from yamlflow.analytics import AnalyticsWarehouse, EXAMPLES, load_snapshot
from yamlflow.agent import answer_question

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
SNAPSHOT = Path(os.environ.get("YAMLFLOW_SNAPSHOT", str(ROOT / "data/analytics_snapshot.json")))

st.set_page_config(page_title="YAMLFlow · Analytics", page_icon="◈", layout="wide")
st.markdown("""<style>
    .block-container {padding-top: 2rem; max-width: 1450px;}
    [data-testid="stMetric"] {border:1px solid #dbe5e9; border-radius:12px; padding:18px; background:#fff;}
    .hero {background:#102d39; border-radius:18px; padding:28px 32px; color:#f3fbfc; margin-bottom:24px;}
    .hero h1 {color:#f3fbfc; font-size:36px; margin:4px 0 8px; padding:0;}
    .hero p {color:#c9e1e8; margin:0;}
    .eyebrow {color:#68d6c2; font-size:12px; letter-spacing:2px; font-weight:700;}
    [data-testid="stMetricValue"] {font-size:clamp(22px, 2.6vw, 34px);}
    @media (max-width:900px) {
        [data-testid="stColumn"]:has([data-testid="stMetric"]) {min-width:calc(50% - 1rem) !important;}
        [data-testid="stMetricLabel"] p {white-space:normal;}
        .hero {padding:22px;}
        .hero h1 {font-size:29px;}
    }
</style>""", unsafe_allow_html=True)

with st.sidebar:
    st.title("◈ YAMLFlow")
    st.caption("From YAML to answers")
    st.divider()
    mode = st.radio("Analysis mode", ["Local demo", "AI agent"])
    model = st.text_input("OpenAI model", value=os.environ.get("OPENAI_MODEL", ""), placeholder="Your tool-capable model ID")
    api_key = st.text_input("OpenAI API key", value=os.environ.get("OPENAI_API_KEY", ""), type="password")
    consent = st.checkbox("Allow the AI agent to send my question, table schema and up to 100 result rows per query to OpenAI.", disabled=mode != "AI agent")
    if mode == "Local demo":
        st.info("Sample questions run locally. No model or API key is needed.")
    else:
        st.caption("The agent can inspect schema, run queries and correct SQL errors. API usage is billed to your configured account.")
    st.divider()
    if st.button("Reload published data", width="stretch"):
        st.rerun()
    if st.button("Clear conversation", width="stretch"):
        st.session_state["history"] = []
        st.rerun()
    st.caption("Read-only analytics · local snapshot")

st.markdown("""<div class="hero"><div class="eyebrow">YAMLFLOW / ANALYTICS WORKSPACE</div>
<h1>Your data. A clearer answer.</h1><p>Explore sales performance, ask questions, and inspect the evidence behind every result.</p></div>""", unsafe_allow_html=True)

if not SNAPSHOT.exists():
    st.warning("No published data yet. Run `make run` and `make publish` from the project terminal, then reload this page.")
    st.stop()
try:
    snapshot = load_snapshot(SNAPSHOT)
    warehouse = AnalyticsWarehouse(snapshot)
except Exception as exc:
    st.error(f"The data snapshot could not be opened: {exc}")
    st.stop()


def frame(result):
    df = pd.DataFrame(result["rows"], columns=result["columns"])
    for column in df.columns:
        if df[column].dtype == object and len(df):
            converted = pd.to_numeric(df[column], errors="coerce")
            if converted.notna().sum() == df[column].notna().sum():
                df[column] = converted
    return df


def render_result(result, key):
    with st.expander("SQL used", expanded=False):
        st.code(result["sql"], language="sql")
    df = frame(result)
    st.dataframe(df, width="stretch", hide_index=True)
    st.caption(f"Snapshot: {result['published_at']} · {len(df)} displayed rows")
    if result["truncated"]:
        st.warning("Showing the first 100 rows. Refine the query; totals must not be calculated from this partial result.")
    if len(df) > 1 and len(df.columns) > 1:
        numerical = list(df.select_dtypes(include="number").columns)
        labels = [col for col in df.columns if col not in numerical]
        if numerical and labels:
            metric = "order_value" if "order_value" in numerical else numerical[0]
            st.bar_chart(df, x=labels[0], y=metric, color="#149b87")
    st.download_button("Download result CSV", df.to_csv(index=False).encode(), "query_result.csv", "text/csv", key=key)


try:
    kpis = warehouse.query("SELECT count(*) AS orders, count(DISTINCT customer_id) AS customers, sum(order_value) AS value, avg(order_value) AS average FROM dw.fact_order")["rows"][0]
    for col, label, value in zip(st.columns(4), ["Submitted order value", "Submitted orders", "Buying customers", "Average order value"],
                                 [f"{kpis['value'] or 0:,.2f}", str(kpis["orders"]), str(kpis["customers"]), f"{kpis['average'] or 0:,.2f}"]):
        col.metric(label, value)
    st.caption(f"Published {snapshot['published_at']} · Order values in source base-currency units; no currency is assumed.")
    ask, explore, sql_tab = st.tabs(["Ask the data", "Explore tables", "SQL workspace"])
    with ask:
        st.subheader("What would you like to understand?")
        st.caption("Choose a sample question or connect an AI model for open-ended questions. Questions are analyzed independently.")
        question = None
        examples = list(EXAMPLES)
        for index, col in enumerate(st.columns(2)):
            with col:
                for example in examples[index::2]:
                    if st.button(example, width="stretch"):
                        question = example
        typed = st.chat_input("Ask about customers, order value, or daily performance…", max_chars=6000)
        question = typed or question
        history = st.session_state.setdefault("history", [])
        if question:
            if mode == "AI agent" and (not api_key or not model or not consent):
                st.warning("Add an API key and model in the sidebar, then enable data sharing to use the AI agent.")
            elif mode == "Local demo" and question not in EXAMPLES:
                st.info("Local demo supports the four sample questions above. Select AI agent for open-ended analysis.")
            else:
                try:
                    with st.spinner("Checking the data…"):
                        if mode == "Local demo":
                            result = warehouse.query(EXAMPLES[question])
                            answer = {"answer": "Here is the verified result from the published snapshot.",
                                      "trace": [{"tool": "run_sql", "status": "success", "sql": result["sql"]}], "results": [result]}
                        else:
                            from openai import OpenAI
                            answer = answer_question(question, warehouse, OpenAI(api_key=api_key, timeout=30, max_retries=1), model)
                    history.append({"question": question, "answer": answer, "mode": mode})
                    del history[:-12]
                except Exception as exc:
                    st.error(f"Analysis could not complete ({type(exc).__name__}). Check your model configuration or try a simpler query.")
        for index, item in enumerate(history):
            with st.chat_message("user"):
                st.write(item["question"])
            with st.chat_message("assistant"):
                st.caption(item["mode"])
                st.markdown(item["answer"]["answer"])
                with st.expander("Analysis steps"):
                    st.json(item["answer"]["trace"])
                for offset, result in enumerate(item["answer"]["results"]):
                    render_result(result, f"chat_{index}_{offset}")
    with explore:
        name = st.selectbox("Curated table", list(snapshot["tables"]))
        st.caption(f"{len(snapshot['tables'][name]['rows'])} rows in the published snapshot")
        with st.expander("Columns and types"):
            st.dataframe(pd.DataFrame(snapshot["tables"][name]["columns"]), hide_index=True, width="stretch")
        render_result(warehouse.query(f"SELECT * FROM {name}"), "table_download")
    with sql_tab:
        st.subheader("Inspect the data directly")
        sql = st.text_area("Read-only SQL", value="SELECT * FROM dw.fact_order ORDER BY order_date", height=150)
        if st.button("Run query", type="primary"):
            try:
                st.session_state["sql_result"] = warehouse.query(sql)
            except Exception as exc:
                st.error(str(exc))
        if "sql_result" in st.session_state:
            render_result(st.session_state["sql_result"], "sql_download")
finally:
    warehouse.close()
