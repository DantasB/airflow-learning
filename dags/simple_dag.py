from datetime import datetime, timedelta
import json
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
doc_md = """
# DAG Example
    This is an example DAG created by BDantas.

## DAG Details
    The DAG is named `simple_dag` and has a start date of `2021-01-01`.
    The DAG is configured to run every day at midnight.
    The DAG is configured with 2 retries.
    The DAG is configured with retry_delay of 3 minutes.
"""
@dag(
    schedule_interval="@hourly",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    doc_md=doc_md,
    default_args={
        "owner": "BDantas",
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
    },
    tags=['example', 'ETL']
)

def simple_dag():
    @task(task_id="extract_data", retries=3) # This decorator tells Airflow that this task is ran using the PythonOperator
    def extract():
        """
        #### Extract task
        A simple "extract" task to get data ready for the rest of the
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict
        
    @task(task_id="transform_data", retries=3)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple "transform" task which takes in the collection of order data and
        """
        transformed_data = {}
        for order_id, order_value in order_data_dict.items():
            transformed_data[order_id] = order_value * 1.1
        return transformed_data
    
    @task(task_id="load_data", retries=3)
    def load(transformed_data: dict):
        """
        #### Load task
        A simple "load" task which takes in the collection of transformed data and
        loads it into a database.
        """
        print(transformed_data)
        return transformed_data

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary)

dag = simple_dag()