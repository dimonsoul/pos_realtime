"""
Task 2: Resilient Airflow Ingestion Pipeline
- Resource Optimization : ใช้ FileSensor(deferrable=True) แทน mode='poke' แบบดั้งเดิม
                           เพื่อคืน Worker Slot ระหว่างรอไฟล์ (Deferrable Operator / Triggerer)
- Idempotency & Cleanse : Upsert ด้วย ON CONFLICT DO UPDATE คีย์ transaction_id
                           รัน DAG ซ้ำกี่ครั้งด้วยไฟล์เดิม ก็ไม่เกิดแถวซ้ำใน Database
- Dynamic Alerting      : แจ้งเตือนอัตโนมัติผ่าน Webhook (Slack / Telegram / Line Notify)
                           เมื่อพบ Anomaly (ยอดขายพุ่ง / ข้อมูลขยะ) ตั้งค่าผ่าน Airflow Variables
"""
from datetime import datetime, timedelta
import os
import glob
import shutil

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

WATCH_DIR = "/tmp/pos_data"
FILE_PATTERN = "pos_data_batch_*.csv"
PROCESSED_DIR = os.path.join(WATCH_DIR, "processed")
POSTGRES_CONN_ID = "postgres_midterm_conn"

FLASH_SALE_ROW_THRESHOLD = 100    # ถือว่าเป็น Flash Sale ถ้าหนึ่งรอบมี record รวม >= ค่านี้
HIGH_SALES_THRESHOLD = 5000.0     # บาท

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pos_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    store_id VARCHAR(20),
    event_timestamp TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    product_id VARCHAR(20),
    product_name VARCHAR(50),
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_reason VARCHAR(200),
    source_file VARCHAR(255)
);
"""

UPSERT_SQL = """
INSERT INTO pos_transactions (
    transaction_id, store_id, event_timestamp, product_id, product_name,
    quantity, unit_price, total_amount, is_anomaly, anomaly_reason, source_file
) VALUES %s
ON CONFLICT (transaction_id) DO UPDATE SET
    store_id = EXCLUDED.store_id,
    event_timestamp = EXCLUDED.event_timestamp,
    product_id = EXCLUDED.product_id,
    product_name = EXCLUDED.product_name,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    total_amount = EXCLUDED.total_amount,
    is_anomaly = EXCLUDED.is_anomaly,
    anomaly_reason = EXCLUDED.anomaly_reason,
    source_file = EXCLUDED.source_file;
"""


def _send_dynamic_alert(message: str):
    """ส่งข้อความแจ้งเตือนผ่าน Webhook ที่เลือกได้แบบ dynamic (Slack / Telegram / Line Notify)
    ตั้งค่า Channel/Token ผ่าน Airflow Variables: ALERT_CHANNEL, ALERT_WEBHOOK_URL,
    ALERT_BOT_TOKEN, ALERT_CHAT_ID, ALERT_LINE_TOKEN
    ถ้ายังไม่ตั้งค่า จะ log ไว้แทนการทำให้ Task ล้มเหลว"""
    channel = Variable.get("ALERT_CHANNEL", default_var="none").lower()

    try:
        if channel == "slack":
            webhook_url = Variable.get("ALERT_WEBHOOK_URL")
            requests.post(webhook_url, json={"text": message}, timeout=10)
        elif channel == "telegram":
            bot_token = Variable.get("ALERT_BOT_TOKEN")
            chat_id = Variable.get("ALERT_CHAT_ID")
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        elif channel == "line":
            token = Variable.get("ALERT_LINE_TOKEN")
            requests.post(
                "https://notify-api.line.me/api/notify",
                headers={"Authorization": f"Bearer {token}"},
                data={"message": message},
                timeout=10,
            )
        else:
            print(f"[ALERT - webhook not configured] {message}")
            return
        print(f"[ALERT sent via {channel}] {message}")
    except Exception as exc:
        # การแจ้งเตือนพลาดต้องไม่ทำให้ pipeline หลักล้มเหลวตาม
        print(f"[ALERT FAILED] {exc} :: {message}")


with DAG(
    dag_id='pos_resilient_pipeline',
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['assignment', 'pos', 'resilient'],
) as dag:

    # 1) Deferrable Operator แทน FileSensor mode='poke' แบบดั้งเดิม
    #    ระหว่างรอไฟล์ Task จะถูก defer ให้ Triggerer ดูแล และคืน Worker Slot ทันที
    wait_for_pos_file = FileSensor(
        task_id='wait_for_pos_file',
        filepath=os.path.join(WATCH_DIR, FILE_PATTERN),
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=240,
        deferrable=True,
    )

    @task
    def ensure_schema():
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(CREATE_TABLE_SQL)

    @task
    def process_and_load_to_postgres():
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        files = sorted(glob.glob(os.path.join(WATCH_DIR, FILE_PATTERN)))
        if not files:
            raise FileNotFoundError("ไม่พบไฟล์ POS ในระบบ")

        frames = []
        for f in files:
            df = pd.read_csv(f)
            df['source_file'] = os.path.basename(f)
            frames.append(df)
        raw = pd.concat(frames, ignore_index=True)

        # --- Data Cleanse ---
        raw['is_anomaly'] = False
        raw['anomaly_reason'] = ''

        # a) Negative Amount -> ทำเครื่องหมาย Anomaly (เก็บไว้ให้ Dashboard highlight ไม่ลบทิ้ง)
        neg_mask = raw['total_amount'] < 0
        raw.loc[neg_mask, 'is_anomaly'] = True
        raw.loc[neg_mask, 'anomaly_reason'] = 'negative_amount'

        # b) Duplicate transaction_id ภายในรอบเดียวกัน -> เก็บ record ล่าสุด ที่เหลือทำเครื่องหมาย anomaly
        dup_mask = raw.duplicated(subset=['transaction_id'], keep='last')
        raw.loc[dup_mask, 'is_anomaly'] = True
        raw.loc[dup_mask, 'anomaly_reason'] = (raw.loc[dup_mask, 'anomaly_reason'] + '|duplicate_id').str.strip('|')
        clean = raw.drop_duplicates(subset=['transaction_id'], keep='last').copy()
        clean['anomaly_reason'] = clean['anomaly_reason'].replace('', None)

        # c) Idempotent Upsert เข้า PostgreSQL (ON CONFLICT DO UPDATE)
        #    -> รัน DAG ซ้ำกี่ครั้งด้วยไฟล์เดิม ก็จะไม่เกิดแถวซ้ำใน Database
        rows = list(clean[[
            'transaction_id', 'store_id', 'timestamp', 'product_id', 'product_name',
            'quantity', 'unit_price', 'total_amount', 'is_anomaly', 'anomaly_reason', 'source_file'
        ]].itertuples(index=False, name=None))

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        from psycopg2.extras import execute_values
        execute_values(cur, UPSERT_SQL, rows)
        conn.commit()
        cur.close()
        conn.close()

        # ย้าย (ไม่ลบ) ไฟล์ที่ประมวลผลแล้ว กัน Sensor จับซ้ำ แต่ยัง rerun DAG ด้วยไฟล์เดิมได้เพื่อพิสูจน์ idempotency
        for f in files:
            shutil.move(f, os.path.join(PROCESSED_DIR, os.path.basename(f)))

        return {
            'total_sales': float(clean['total_amount'].sum()),
            'total_rows': int(len(clean)),
            'anomaly_count': int(clean['is_anomaly'].sum()),
            'files_processed': [os.path.basename(f) for f in files],
        }

    @task.branch
    def evaluate_alert(metrics: dict):
        if (
            metrics['anomaly_count'] > 0
            or metrics['total_rows'] >= FLASH_SALE_ROW_THRESHOLD
            or metrics['total_sales'] >= HIGH_SALES_THRESHOLD
        ):
            return 'send_dynamic_alert'
        return 'no_alert_needed'

    @task
    def send_dynamic_alert(metrics: dict):
        message = (
            f"POS Alert - rows={metrics['total_rows']} "
            f"sales={metrics['total_sales']:.2f} THB "
            f"anomalies={metrics['anomaly_count']} "
            f"files={metrics['files_processed']}"
        )
        _send_dynamic_alert(message)

    no_alert_needed = EmptyOperator(task_id='no_alert_needed')

    schema_ready = ensure_schema()
    metrics_data = process_and_load_to_postgres()
    branch_decision = evaluate_alert(metrics_data)

    wait_for_pos_file >> schema_ready >> metrics_data >> branch_decision
    branch_decision >> send_dynamic_alert(metrics_data)
    branch_decision >> no_alert_needed
