"""Task 4 Benchmark - FileSensor mode='reschedule' (คืน Worker Slot ระหว่างรอ แต่ยัง sleep ที่ scheduler)"""
from datetime import datetime
import os
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

BENCH_DIR = "/tmp/bench_data"

with DAG(
    dag_id='benchmark_filesensor_reschedule',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['benchmark'],
) as dag:
    wait = FileSensor(
        task_id='wait_reschedule',
        filepath=os.path.join(BENCH_DIR, 'ready_reschedule.flag'),
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=180,
        mode='reschedule',
    )

    @task
    def done():
        print("reschedule-mode sensor satisfied")

    wait >> done()
