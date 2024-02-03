import os
import logging

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

def get_logger():

    logging.basicConfig(
        format='%(asctime)s - %(levelname)-8s - %(filename)s:%(lineno)s - %(message)s'
    )

    logger = logging.getLogger('producer')
    logger.setLevel(LOG_LEVEL)

    return logger