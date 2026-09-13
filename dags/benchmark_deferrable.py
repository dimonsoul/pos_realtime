"""Task 4 Benchmark - FileSensor(deferrable=True) (ปล่อย Worker Slot ทั้งหมดให้ Triggerer ดูแล)"""
from datetime import datetime
import os
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

BENCH_DIR = "/tmp/bench_data"

with DAG(
    dag_id='benchmark_filesensor_deferrable',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['benchmark'],
) as dag:
    wait = FileSensor(
        task_id='wait_deferrable',
        filepath=os.path.join(BENCH_DIR, 'ready_deferrable.flag'),
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=180,
        deferrable=True,
    )

    @task
    def done():
        print("deferrable sensor satisfied")

    wait >> done()
