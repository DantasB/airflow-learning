from datetime import datetime, timedelta
import json
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

doc_md = """
# DAG Example
    This is an example DAG created by BDantas.

## DAG Details
    The DAG is named `simple_dag` and has a start date of `2021-01-01`.
    The DAG is configured to run every day at midnight.
    The DAG is configured with 0 retries.
    The DAG is configured with retry_delay of 3 minutes.
"""

DEFAULT_TASK_PARAMETERS = {
    "owner": "BDantas",  # The owner of the DAG is BDantas
    "retries": 3,  # If a task fails, it will retry 0 times.
    "retry_delay": timedelta(
        minutes=3
    ),  # If a task fails, it will retry every 3 minutes.
}


@dag(
    schedule_interval="@hourly",  # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG will run hourly
    start_date=datetime(
        2021, 1, 1
    ),  # This DAG will run for the first time on January 1, 2021. Best practice is to use a static start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    catchup=False,  # This DAG will only run for the latest schedule_interval.
    doc_md=doc_md,  # This is the documentation for your DAG. It will be displayed in the Airflow UI
    default_args=DEFAULT_TASK_PARAMETERS,
    tags=[
        "example",
        "ETL",
    ],  # If set, this tag is shown in the DAG view of the Airflow UI
    max_active_runs=1,  # This limits the number of DAG runs to 1.
)
def simple_dag():
    @task.python(
        task_id="extract_data", retries=3
    )  # This decorator tells Airflow that this task is ran using the PythonOperator
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

    @task.python(
        task_id="transform_data", retries=3
    )  # This decorator tells Airflow that this task is ran using the PythonOperator
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple "transform" task which takes in the collection of order data and
        """
        transformed_data = {}
        for order_id, order_value in order_data_dict.items():
            transformed_data[order_id] = order_value * 1.1
        return transformed_data

    @task.python(
        task_id="load_data", retries=3
    )  # This decorator tells Airflow that this task is ran using the PythonOperator
    def load(transformed_data: dict):
        """
        #### Load task
        A simple "load" task which takes in the collection of transformed data and
        loads it into a database.
        """
        with open("/tmp/transformed_data.json", "w") as f:
            json.dump(transformed_data, f)

    waiting_for_load = FileSensor(
        task_id="waiting_for_load",
        fs_conn_id="fs_default",
        filepath="transformed_data.json",
    )

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary)
    waiting_for_load


dag = simple_dag()
