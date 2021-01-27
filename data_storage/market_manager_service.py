import logging
import os
import sys

from data_storage.aws.s3.S3Service import S3Service
from data_storage.currency.CurrencyService import CurrencyService
from data_storage.symbol.Symbol import Symbol


class SymbolManager:
    local_storage_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']
    s3_helper = S3Service()
    currency_service = CurrencyService()
    local_symbols = {}

    def process_symbols(self):
        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=False)
        logging.info(f"Synced markets with s3 and start sync with platform.")

        result_exchange_info = self.currency_service.pull_exchange_info()
        symbol = None
        for symbol_info in result_exchange_info['symbols']:
            try:
                if symbol_info['name'] not in self.local_symbols:
                    symbol = Symbol(symbol_info)
                    #self.local_symbols[symbol.id] = symbol
                    logging.info(f"Create new symbol:{symbol.name} in memory.")
                else:
                    #symbol = self.local_symbols[symbol_info['name']]
                    logging.info(f"Found existing symbol:{symbol.name} in memory.")

                symbol.process_symbol(self.currency_service)
            except Exception as err:
                logging.error(f"Failed to update Symbol:{symbol_info}", exc_info=sys.exc_info())

        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=True)

        logging.info(f"Synced markets with s3 after processing.")
