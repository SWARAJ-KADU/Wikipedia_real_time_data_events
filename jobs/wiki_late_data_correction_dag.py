from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="wiki_late_data_correction",
    start_date=datetime(2025, 12, 21),
    schedule_interval="None",  # hourly
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "late-data"],
) as dag:

    late_data_correction = BashOperator(
        task_id="late_data_correction",
        bash_command="""
        python iceberg/late_records.py
        """
    )
