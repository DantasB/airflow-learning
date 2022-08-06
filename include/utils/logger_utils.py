import logging

LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def get_logger_with_date_output(logger_name: str) -> logging.Logger:
    """Get a logger with a date output

    Args:
        logger_name (str): The name of the logger

    Returns:
        logging.Logger: The logger
    """

    logging.basicConfig(format=LOGGING_FORMAT)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.debug(f"Logger {logger_name} created")
    return logger
