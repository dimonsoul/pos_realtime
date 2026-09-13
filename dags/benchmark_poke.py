"""Task 4 Benchmark - FileSensor mode='poke' (แบบดั้งเดิม: ยึด Worker Slot ตลอดเวลาที่รอ)"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

BENCH_DIR = "/tmp/bench_data"

with DAG(
    dag_id='benchmark_filesensor_poke',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['benchmark'],
) as dag:
    wait = FileSensor(
        task_id='wait_poke',
        filepath=os.path.join(BENCH_DIR, 'ready_poke.flag'),
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=180,
        mode='poke',
    )

    @task
    def done():
        print("poke-mode sensor satisfied")

    wait >> done()
