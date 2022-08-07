import os
import logging
import pytest
from airflow_learning.include.utils.env_utils import get_env_variable

LOGGER = logging.getLogger(__name__)


class TestEnvUtils:
    def test_get_env_variable_error(self, caplog):
        with pytest.raises(RuntimeError):
            with caplog.at_level(logging.INFO):
                get_env_variable("NON_EXISTENT_VARIABLE")
        assert "Getting environment variable..." in caplog.text

    def test_get_env_variable_success(self, caplog):
        os.environ["TEST_GET_ENV_VARIABLE"] = "TEST_GET_ENV_VARIABLE"
        variable = get_env_variable("TEST_GET_ENV_VARIABLE")
        with caplog.at_level(logging.INFO):
            variable = get_env_variable("TEST_GET_ENV_VARIABLE")
        assert variable == "TEST_GET_ENV_VARIABLE"
        assert "Getting environment variable..." in caplog.text
        assert "Got value for TEST_GET_ENV_VARIABLE" in caplog.text
