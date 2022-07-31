from datetime import datetime
from airflow.models.baseoperator import (
    chain,
)
from airflow.decorators import dag
from dags.tasks.etl_tasks import load_data, transform_data, extract_data

@dag(
    schedule_interval="@daily",
    start_date=datetime(2022,1,1),
    catchup=False,
)
def cleaner_dag():
    chain(load_data(), transform_data(), extract_data())

dag = cleaner_dag()