from os import environ

from airflow_learning.include.utils.logger_utils import get_logger_with_date_output

logger = get_logger_with_date_output("env_utils")


def get_env_variable(variable_name):
    """Get environment variable value.

    Args:
        variable_name (str): Environment variable name.

    Raises:
        Exception: If environment variable is not set.

    Returns:
        str: Environment variable value.
    """
    logger.info("Getting environment variable...")

    variable_key = environ.get(variable_name)
    if not variable_key:
        logger.error(f"Variable {variable_name} not found")
        raise RuntimeError(f"{variable_name} is not set")

    logger.info(f"Got value for {variable_name}")

    return variable_key
