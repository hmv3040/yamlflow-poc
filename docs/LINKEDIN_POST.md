# LinkedIn launch copy

## Main post

What if adding a data model also created its Airflow task?

I built **YAMLFlow**, a local Data Engineering POC to explore that idea.

The workflow:

Source YAML → DuckDB raw tables → DW models and quality checks → metrics → Streamlit analytics.

The same YAML configuration also generates the Airflow DAG, including dependencies,
retries, quality gates, and a final analytics snapshot publisher.

What you can try:

• Change synthetic source data and watch the metrics update.
• Define transformations in SQL inside YAML.
• Inspect the generated 11-task Airflow DAG.
• Explore tables, run read-only SQL, and view charts in Streamlit.
• Optionally connect an OpenAI model for an agent that inspects schema, runs SQL,
  and can correct failed queries—with the SQL and results visible.

The basic demo needs no SaaS account or API key. Airflow is optional for the first
run, and the sample questions execute locally without a model.

This is a learning POC: full refreshes, synthetic fixtures, and a single-machine
setup. The local suite has 24 passing tests, and the generated Airflow workflow
completed all 11 tasks. Live ERPNext/model integrations still need account-level
validation.

Repository + setup instructions:
https://github.com/hmv3040/yamlflow-poc

If you try it, I'd appreciate feedback on the YAML design, generated DAG, and
analytics experience. What would you change first?

#DataEngineering #ApacheAirflow #DuckDB #Python #Streamlit

## Optional first comment

If you'd like to try it:

1. Clone the repository.
2. Create a Python virtual environment and install the analytics dependencies.
3. Run `make demo` to build the sample data and open Streamlit.
4. For Airflow, run `make up`, then `make verify-airflow`.

The walkthrough includes expected totals, sample SQL, and exercises to change data
and add a quality check:
https://github.com/hmv3040/yamlflow-poc/blob/main/docs/QUICKSTART.md

## Suggested attachment

Use `docs/images/analytics-overview.png` as the screenshot. The repository README
also includes the architecture diagram. No credentials or warehouse files should
be attached to the post.
