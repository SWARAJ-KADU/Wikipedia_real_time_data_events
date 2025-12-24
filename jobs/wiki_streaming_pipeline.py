from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="wiki_streaming_pipeline",
    start_date=datetime(2025, 12, 21),
    schedule_interval=None,   # MANUAL
    catchup=False,
    max_active_runs=1,
    tags=["spark", "streaming", "iceberg"],
) as dag:

    start_streaming_pipeline = BashOperator(
        task_id="start_streaming_pipeline",
        bash_command="""
        python streaming/main.py
        """
    )
