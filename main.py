import logging
from logging.config import fileConfig
import os

fileConfig(os.path.dirname(os.path.realpath(__file__)) + '/logging_config.ini')
logger = logging.getLogger()

if __name__ == '__main__':
    logging.info("Start processing")
