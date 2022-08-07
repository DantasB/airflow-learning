import json
from typing import Any, Dict
import requests
from airflow.decorators import task
from include.utils.env_utils import get_env_variable
from include.utils.logger_utils import get_logger_with_date_output

logger = get_logger_with_date_output("data_ingestion")


@task.python
def get_users() -> Dict[Any, Any]:
    """Get users from randomuser.me API

    Raises:
        Exception: If API call fails

    Returns:
        Dict[Any, Any]: Users data
    """

    logger.info("Getting users...")

    number_of_users = get_env_variable("NUMBER_OF_USERS")
    response = requests.get(f"https://randomuser.me/api/?results={number_of_users}")
    if not response.ok:
        logger.error(f"Error getting users: {response.text}")
        raise Exception("Failed to get users")

    logger.info("Got users!")

    return response.json()


@task.python
def save_users_data_as_json(users: Dict[Any, Any]) -> None:
    """Save users data as json

    Args:
        users (Dict[Any, Any]): Users data
    """
    logger.info("Saving users data as json...")

    with open("users.json", "w", encoding="utf-8") as file:
        json.dump(users, file)

    logger.info("Saved users data as json!")
