from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="wiki_iceberg_maintenance",
    start_date=datetime(2025, 12, 21),
    schedule_interval="0 */2 * * *",  # every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=["iceberg", "maintenance"],
) as dag:

    iceberg_maintenance = BashOperator(
        task_id="iceberg_maintenance",
        bash_command="""
        python iceberg/iceberg_maintenance.py
        """
    )
