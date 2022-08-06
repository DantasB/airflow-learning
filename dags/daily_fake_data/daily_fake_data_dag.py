from datetime import datetime, timedelta
from airflow.models.baseoperator import (
    chain,
)

from airflow.decorators import dag
from airflow.utils.edgemodifier import Label

from daily_fake_data.tasks.data_ingestion import (
    get_users,
    save_users_data_as_json,
)

@dag(
    dag_id="daily-fake-data",
    default_args={"owner": "BDantas"},
    tags=["ingestion", "random-users"],
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    dagrun_timeout=timedelta(minutes=10),
    catchup=False,
)
def daily_fake_data_dag():
    """
        # Daily Fake Data
        This DAG will get fake users and save them as a csv file.

        ## DAG Task
        The DAG have the following tasks:

        - get_users: Get users from randomuser.me API
        - save_users_data_as_csv: Save users data as csv
    """

    users = get_users()

    chain(
        users,
        Label("Save Data as JSON"),
        save_users_data_as_json(users),
    )


DAG = daily_fake_data_dag()
