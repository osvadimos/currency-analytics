import logging
import os
from logging.config import fileConfig

from data_storage.data.DeployService import DeployService

fileConfig(os.path.dirname(os.path.realpath(__file__)) + '/logging_config.ini')
logger = logging.getLogger()

if __name__ == '__main__':
    logging.info("Start deploying")
    deploy_service = DeployService()
    deploy_service.deploy_data()