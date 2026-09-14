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

This repo holds the dashboard + DAG source for submission and Streamlit Cloud deployment.
The Airflow/PostgreSQL dev stack (`docker-compose.yml`) that produces `pos_transactions`
lives in a separate local project — **do not add a `docker-compose.yml` here** with the
same container names, or it will collide with (and blank out) that stack's Postgres volume.

## Running the dashboard locally

```bash
pip install -r requirements.txt
streamlit run dashboard_app.py
```

The dashboard reads `POS_DB_URL` (env var locally, or `st.secrets["POS_DB_URL"]` on
Streamlit Community Cloud). Point it at whichever Postgres currently holds
`pos_transactions` — a local Docker instance, or the Neon database used for the
Streamlit Cloud deployment.

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
