from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.decorators import task
from airflow.decorators import task
from include.utils.logger_utils import get_logger_with_date_output

logger = get_logger_with_date_output("xcom_cleaner")

@task.python
@provide_session
def cleanup_xcoms(dag_id, session=None):
    """ Delete every xcom created by a dag

    Args:
        dag_id (str): The identifier of the dag
    """
    logger.info(f"Cleaning XCOMs created by DAG: {dag_id}")

    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

    logger.info(f"XCOMs created by DAG: {dag_id} were cleaned.")
