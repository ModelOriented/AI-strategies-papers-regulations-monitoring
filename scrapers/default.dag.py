from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from dotenv import dotenv_values
import os

args = {"owner": "piotrp", "retries": 1, "retry_delay": timedelta(minutes=60)}

dag = DAG(
    dag_id="scrapers",
    default_args=args,
    max_active_runs=1,
    start_date=datetime(2021, 9, 15),
    catchup=False,
    schedule_interval="0 0 * * 0",
)

oecd = BashOperator(
    task_id="OECD", bash_command="python /mair/oecd-scraping.py", dag=dag
)

eurlex = BashOperator(
    task_id="EURLEX", bash_command="python /mair/eurlex-scraping.py", dag=dag
)

[oecd, eurlex]
