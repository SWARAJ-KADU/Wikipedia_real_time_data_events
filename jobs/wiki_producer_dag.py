from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="wiki_kafka_producer",
    start_date=datetime(2025, 12, 21),
    schedule_interval="None",
    catchup=False,
    tags=["kafka", "ingestion", "wiki"],
) as dag:

    start_kafka_producer = BashOperator(
        task_id="start_wiki_kafka_producer",
        bash_command="""
        python kafka/ingestion.py
        """
    )
