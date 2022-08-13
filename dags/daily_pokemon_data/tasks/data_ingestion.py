# -*- coding: utf-8 -*-
import json
from os import path
from typing import Any, Dict
import requests
from airflow.decorators import task
from include.utils.env_utils import get_env_variable
from include.utils.logger_utils import get_logger_with_date_output

logger = get_logger_with_date_output("data_ingestion")


@task.python
def get_pokemons() -> Dict[Any, Any]:
    """Get pokemons from pokeapi.co API

    Raises:
        Exception: If API call fails

    Returns:
        Dict[Any, Any]: Pokemons data
    """
    logger.info("Getting users...")

    pokemon_api_route = get_env_variable("POKEMON_API_ROUTE")
    number_of_pokemons = get_env_variable("NUMBER_OF_POKEMONS")
    response = requests.get(f"{pokemon_api_route}/pokemon?limit={number_of_pokemons}")
    if not response.ok:
        raise Exception("Failed to get pokemons")

    logger.info("Got users!")

    return response.json()


@task.python
def save_pokemons_data_as_json(pokemons: Dict[Any, Any]) -> None:
    """Save pokemons data as json

    Args:
        pokemons (Dict[Any, Any]): pokemons data
    """
    logger.info("Saving pokemons data as json...")

    save_path = path.join(get_env_variable("JSON_PATH"), "pokemons.json")

    with open(save_path, mode="w+", encoding="utf-8") as output:
        json.dump(pokemons, output, indent=4)

    logger.info("Saved pokemons data as json!")
