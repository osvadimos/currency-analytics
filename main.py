import logging
from logging.config import fileConfig
import os
from data_storage.market_manager_service import SymbolManager

fileConfig(os.path.dirname(os.path.realpath(__file__)) + '/logging_config.ini')
logger = logging.getLogger()

if __name__ == '__main__':
    logging.info("Start processing")
    #todo add endless loop
    symbols_manager = SymbolManager()
    symbols_manager.process_symbols()