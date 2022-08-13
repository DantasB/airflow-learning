# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from airflow.models.baseoperator import (
    chain,
)

from airflow.decorators import dag
from airflow.utils.edgemodifier import Label

from daily_pokemon_data.tasks.data_ingestion import (
    get_pokemons,
    save_pokemons_data_as_json,
)

from dags.general_tasks.xcom_cleaner import cleanup_xcoms


@dag(
    dag_id="daily-pokemon-data",
    default_args={"owner": "BDantas"},
    tags=["ingestion", "pokemon"],
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    dagrun_timeout=timedelta(minutes=10),
    catchup=False,
)
def daily_pokemon_data_dag():
    """
    # Daily Pokemon Data
    This DAG will get pokemons and save them as a csv file.

    ## DAG Task
    The DAG have the following tasks:

    - get_pokemons: Get pokemons from pokeapi.co API;
    - save_pokemons_data_as_json: Save pokemons data as json;
    - cleanup_xcoms: Deletes every xcom created by this dag.
    """

    pokemons = get_pokemons()

    chain(
        pokemons,
        Label("Save Data as JSON"),
        save_pokemons_data_as_json(pokemons),
        Label("Cleanup Every XCOM"),
        cleanup_xcoms("{{dag.dag_id}}"),
    )


DAG = daily_pokemon_data_dag()
