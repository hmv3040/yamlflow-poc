# Verification

Recorded on 17 November 2026 with the bundled synthetic sales fixtures.

## Automated checks

- **24 tests passed** across pipeline, compiler, analytics, and Streamlit behavior.
- The generated Airflow 3.1.3 workflow completed **all 11 tasks**, including
  `publish_analytics_snapshot`.
- The Streamlit UI was checked in a browser: KPIs, sample questions, query result
  tables, SQL inspection, and customer order-value charts rendered successfully.

| Output | Expected result |
| --- | ---: |
| Raw customers | 3 |
| Raw orders | 6 |
| Submitted orders in DW | 4 |
| Submitted order value | 2,800 |
| 15 November order value | 2,000 |
| 16 November order value | 800 |

## What the tests cover

- Reruns and source updates/deletions without duplicate raw records.
- Primary-key violations and rollback after failed model quality checks.
- Invalid dependency graphs and deterministic DAG compilation.
- Snapshot publication and curated analytics queries.
- Frappe API pagination and missing credentials through mocked HTTP calls.
- Rejection of write SQL, file readers, system catalogs, secret inspection,
  unknown functions, recursive CTEs, and multiple SQL statements.
- A simulated Responses API tool loop that inspects schema, receives a rejected
  write query, corrects it to a SELECT, and produces an evidence-backed answer.
- Streamlit sample questions, SQL restrictions, and missing AI configuration.

## Verification limits

- **No live ERPNext account was used.** The default source is synthetic fixtures;
  the REST connector still needs validation against a configured ERPNext instance.
- **No live OpenAI call was made.** Agent tests use a simulated client; real usage
  requires your API key and a tool-capable model ID.
- Local demo questions are predefined SQL queries, not LLM-generated answers.
- Full refresh only; incremental ingestion and backfill semantics are not implemented.
- This is a single-machine POC, not a production or multi-user deployment.

## Reproduce

After following [the quickstart](docs/QUICKSTART.md):

```bash
make test
make run
make publish
make report
```

With Docker running, `make up` and `make verify-airflow` reproduce the bundled
Airflow check. The test script expects its original eleven-task shape.
