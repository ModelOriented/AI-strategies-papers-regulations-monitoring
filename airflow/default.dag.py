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
    task_id="OECD", bash_command="python /mair/scripts/oecd-scraping.py", dag=dag
)

eurlex = BashOperator(
    task_id="EURLEX", bash_command="python /mair/scripts/eurlex-scraping.py", dag=dag
)

segment_pdf_and_html = BashOperator(
    task_id="segment_pdf_and_html",
    bash_command="python /mair/scripts/segment_pdf_and_html.py",
    dag=dag,
)

split_to_sentences = BashOperator(
    task_id="split_to_sentences",
    bash_command="python /mair/scripts/processing/split_to_sentences.py all",
    dag=dag,
)

[oecd, eurlex] >> segment_pdf_and_html >> split_to_sentences
