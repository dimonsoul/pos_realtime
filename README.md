# Real-Time Data Pipeline Architecture & Anomaly Detection System for Enterprise POS

240-513 Data Engineering Principles and Applications
นายมาหะมะ กาลาแต — 6910120019

Event-driven POS transaction pipeline built on Apache Airflow (deferrable operators,
idempotent upsert, dynamic alerting) and PostgreSQL, with a Streamlit dashboard for
real-time anomaly monitoring.

## Contents

| Path | Task | Description |
|---|---|---|
| [`pos_generator.py`](pos_generator.py) | Task 1 | Simulates POS workload with edge cases: flash sales (10x spikes), late-arriving data, duplicate transaction IDs, negative amounts |
| [`dags/pos_resilient_pipeline.py`](dags/pos_resilient_pipeline.py) | Task 2 | Airflow DAG — deferrable `FileSensor`, idempotent `ON CONFLICT` upsert into `pos_transactions`, dynamic Slack/Telegram/Line alerting |
| [`dags/benchmark_*.py`](dags) | Task 4 | Controlled benchmark DAGs comparing `FileSensor` poke / reschedule / deferrable modes |
| [`dashboard_app.py`](dashboard_app.py) | Task 3 | Streamlit dashboard: KPIs, moving-average chart, anomaly highlighting, auto-refresh |
| [`docs/`](docs) | Task 4 | Architectural report (benchmark results + scalability proposal), presentation slides, dashboard screenshots |
| `docker-compose.yml` | — | Self-contained Airflow 3.0.2 + PostgreSQL 16 dev stack, scoped to this folder only |

Everything — generator, DAGs, dashboard, and the dev stack — runs from **this folder**.
Container names (`pos-airflow`, `pos-postgres`) and ports (Airflow UI `8082`, Postgres
`5433`) are namespaced so this stack never collides with any other Airflow project on
the same machine.

## Running the full stack locally

```bash
docker compose up -d                     # Airflow UI: http://localhost:8082 (admin/admin123)
python pos_generator.py                  # writes CSV batches into tmp/pos_data (run from THIS folder)
pip install -r requirements.txt
streamlit run dashboard_app.py           # http://localhost:8502
```

`pos_resilient_pipeline` picks up new batches on its 5-minute schedule (or trigger it
manually from the Airflow UI / CLI for an immediate demo run) and loads them into
`pos_transactions` on `localhost:5433`.

The dashboard reads `POS_DB_URL` (env var locally, or `st.secrets["POS_DB_URL"]` on
Streamlit Community Cloud) — defaults to `localhost:5433` (this stack's Postgres). For
the Streamlit Cloud deployment it's pointed at a free [Neon](https://neon.tech) Postgres
instance instead, since Streamlit Cloud cannot reach a database on `localhost`.

## Deploying the dashboard (Streamlit Community Cloud)

1. Push this repo to GitHub, sign in to [share.streamlit.io](https://share.streamlit.io) with GitHub.
2. New app → select this repo → main file: `dashboard_app.py`.
3. App settings → Secrets → add `POS_DB_URL` pointing at a publicly reachable Postgres
   (this deployment uses a free [Neon](https://neon.tech) Postgres instance, since
   Streamlit Cloud cannot reach a database on `localhost`).

## Benchmark result summary (Task 4)

| Mode | Observed `task_instance.state` while waiting | Worker slot usage |
|---|---|---|
| `FileSensor(mode='poke')` | `running` continuously | Occupied 100% of wait time |
| `FileSensor(mode='reschedule')` | toggles `up_for_reschedule` ↔ `running` | Released between pokes |
| `FileSensor(deferrable=True)` | `deferred` continuously | Not used at all — handled by the Triggerer |

Full analysis and the scalability proposal (10,000 TPS via Kafka + columnar DB) are in
[`docs/นายมาหะมะ กาลาแต_6910120019_POS_Architectural_Report.docx`](docs/นายมาหะมะ%20กาลาแต_6910120019_POS_Architectural_Report.docx).
