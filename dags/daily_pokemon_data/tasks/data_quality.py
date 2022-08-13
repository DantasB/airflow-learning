# -*- coding: utf-8 -*-
from os import path
import great_expectations as ge
from airflow.decorators import task

from include.utils.env_utils import get_env_variable


@task.python
def data_quality() -> bool:
    """
    This is a fake data quality task.
    """
    df = ge.read_json(path.join(get_env_variable("JSON_PATH"), "pokemons.json"))

    # Validate if there's no empty values in the dataframe
    expectation = df.expect_column_values_to_not_be_null("results")

    return expectation.success
