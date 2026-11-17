# Try YAMLFlow and make it your own

The shortest path uses synthetic sales data and runs entirely on your computer.
You do not need a SaaS account, Docker, or an API key for this path.

## 1. Clone and install

Use Python 3.11 (recommended), Git, and Make on macOS/Linux. Use WSL2 on Windows;
the current warehouse coordination uses Unix file locks.

```bash
git clone https://github.com/hmv3040/yamlflow-poc.git
cd yamlflow-poc
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[analytics]' pytest
```

No `.env` file is required for the default demo. Do not put credentials in YAML.

## 2. Build the pipeline and open the app

```bash
make demo
```

This builds the warehouse, publishes a snapshot, and starts Streamlit at
[http://127.0.0.1:8501](http://127.0.0.1:8501). Leave that terminal running while
using the app. Press Ctrl+C when finished. To reopen without rebuilding data:

```bash
make analytics
```

Expected result: **3 customers**, **6 raw orders**, **4 submitted orders**, and
**2,800** in submitted order value. Draft and cancelled orders are excluded from
the DW facts. Values are synthetic base-currency units, not recognized revenue.

Use **Local demo** to run four predefined questions without any model calls.
The answers expose the SQL, rows, and charts. **Explore tables** previews curated
data; **SQL workspace** runs your own read-only SELECT queries.

For example:

```sql
SELECT customer_name, SUM(order_value) AS submitted_order_value
FROM dw.fact_order
GROUP BY customer_name
ORDER BY submitted_order_value DESC;
```

## 3. Run the checks

In a second terminal at the project root:

```bash
source .venv/bin/activate
make test
make report
make plan
```

The recorded local suite has **24 passing tests**. See [verification](../VERIFICATION.md)
for what was checked and which integrations remain unverified.

## 4. Play with the project

### Change the source data

1. Stop Streamlit with Ctrl+C (or use a second terminal).
2. Open `examples/sales/fixtures/orders.json`.
3. Change `SO-001`'s `base_net_total` from `1200` to `1500`.
4. Run `make run` and `make publish`.
5. Start/reload the app. Total submitted order value should now be **3,100**.
6. Run the pipeline again: the number of raw orders should remain **6**.

To restore the original sample: `git restore examples/sales/fixtures/orders.json`,
then run `make run` and `make publish`. This command discards edits to that fixture.

### Try a quality check

In `examples/sales/dw/fact_order.yml`, add the following under `tests`:

```yaml
  - {kind: not_null, column: order_value}
```

Run `make run` and `make test`. Inspect the existing unique/non-null checks and
the model SQL. Try changing the SQL to produce duplicate order IDs: the build
should fail and retain the last successful version of that DW table. Earlier
source/model tasks may already have committed; the pipeline is not one global
transaction.

### Generate the Airflow DAG without starting Airflow

```bash
make compile
```

Inspect `generated/dags/sales_analytics.py`. Dependencies and quality gates came
from the project YAML. Generated files are ignored by Git; regenerate after edits.

## 5. Optional: execute with Airflow

Install and start Docker with Compose support, then:

```bash
make up
make verify-airflow
```

The verification command waits for readiness, checks DAG imports, triggers a run,
and checks all **11 tasks** in the bundled demo. It assumes the bundled task count;
if you add models, use Airflow's UI to inspect and trigger your modified DAG.

Open [http://localhost:8081](http://localhost:8081), username `admin`. Retrieve the
generated local password with:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

The DAG starts paused in a fresh installation; `make verify-airflow` unpauses it
and enables its configured hourly schedule. The final task publishes the snapshot
that Streamlit reads. Click **Reload published data** in the app after a new run.

To stop Airflow and retain its metadata and warehouse:

```bash
docker compose down
```

This is a single-machine development setup. If Docker has a small memory allocation,
avoid running another Airflow stack alongside it. Port 8081 is Airflow; port 8501
is the separate Streamlit process.

## 6. Optional: enable the AI agent

In Streamlit, expand the sidebar, select **AI agent**, and enter:

- Your OpenAI API key.
- A tool-capable model ID available to your API account.

Alternatively, copy `.env.example` to `.env` and set `OPENAI_API_KEY` and
`OPENAI_MODEL`, then restart Streamlit. Enable the data-sharing checkbox before
asking a question. Your question, schema and bounded query results are sent to
OpenAI, and usage is billed to your account. Never commit your `.env` file.

The agent can inspect schema, execute a bounded SELECT, receive a SQL error and
retry. Queries are restricted to the curated snapshot. No live model call has
been verified without credentials; the orchestration tests use a simulated client.
Each question is handled independently rather than carrying earlier chat context.

## Troubleshooting

| Symptom | What to try |
| --- | --- |
| `.venv/bin/python` is missing | Create the virtual environment and install dependencies from step 1. |
| `make` is not found | Install Make/developer tools, or use WSL2 on Windows. |
| No analytics snapshot | Run `make run`, then `make publish`. |
| App shows old results | Publish again, then click Reload published data. Existing chat answers retain their original snapshot timestamps. |
| Warehouse is busy | Let the Airflow run finish before opening the database directly. Streamlit can continue reading its separate snapshot. |
| Port 8501 is occupied | Stop the other app, or run `.venv/bin/python -m streamlit run streamlit_app.py --server.port 8502`. |
| Docker API/task timeouts | Free Docker resources and inspect `docker compose logs airflow`. |
| Custom SQL is rejected | Use a single SELECT, schema-qualified curated tables, and supported analytical functions. External files/system tables are intentionally unavailable. |

For live ERPNext setup and framework details, see the [README](../README.md).
