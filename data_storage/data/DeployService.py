import json
import logging
import os
from datetime import datetime, timezone, timedelta

from data_storage.aws.s3.S3Service import S3Service
from data_storage.currency.CurrencyService import CurrencyService
# todo add explicit logging
from data_storage.symbol.Symbol import Symbol


class DeployService:
    exchange_info_file_path = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], "exchange_info.json")

    symbol_columns = ["open_time", "open", "high", "low", "close", "volume"]

    def __init__(self):
        self.currency_service = CurrencyService()
        self.s3_helper = S3Service()

    # todo test
    def deploy_data(self):
        # todo update from s3 first
        logging.info(f"Start sync with s3 s3")

        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'],
                                             is_local_to_s3=False)

        result_exchange_info = self.currency_service.pull_exchange_info()
        if not self.is_data_same(self.exchange_info_file_path, result_exchange_info):
            logging.info(f"Markets data is not the same.")
            self.update_symbols_data(self.exchange_info_file_path, result_exchange_info)
        else:
            logging.info(f"Markets has not changed since last time.")

        for symbol_information in result_exchange_info['symbols']:
            symbol = Symbol(symbol_information)
            if symbol.status == 'TRADING':
                logging.info(f"Trading for symbol:{symbol.name} is open. Start updating.")
                symbol.upgrade_symbol_data(self.currency_service)
            else:
                logging.info(f"Trading for symbol:{symbol.name} is closed.")

        logging.info(f"Synchronized all updated data with s3")
        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=True)

    # todo test method
    @staticmethod
    def is_data_same(absolute_file_path, current_info) -> bool:
        if not os.path.exists(absolute_file_path):
            return False

        with open(absolute_file_path, 'r') as exchange_file:
            stored_data = exchange_file.read()
            exchange_file.close()

        return len(current_info['symbols']) == len(json.loads(stored_data)['symbols'])

    # todo create a test
    # todo where little file is getting created on every test run
    @staticmethod
    def update_symbols_data(absolute_file_path, current_info):

        with open(absolute_file_path, 'r') as exchange_file:
            stored_data = exchange_file.read()
            exchange_file.close()
        new_symbols = {}
        old_symbols = {}
        for new_symbol in current_info['symbols']:
            new_symbols[f"{new_symbol['symbol']}"] = new_symbol

        for old_symbol in json.loads(stored_data)['symbols']:
            old_symbols[f'{old_symbol["symbol"]}'] = old_symbol

        refactored_symbols = []
        old_symbols_copy = old_symbols.copy()
        for symbol_key in old_symbols_copy:
            symbol_value = old_symbols_copy[symbol_key]
            if symbol_key in new_symbols:
                refactored_symbols.append(new_symbols[symbol_key])
            else:
                logging.info(f"Appending new symbol:{symbol_key} to list")
                refactored_symbols.append(symbol_value)
            del old_symbols[symbol_key]

        for symbol_key in new_symbols:
            if symbol_key not in refactored_symbols:
                refactored_symbols.append(new_symbols[symbol_key])

        with open(absolute_file_path, 'w') as exchange_file:
            current_info['symbols'] = refactored_symbols
            exchange_file.write(json.dumps(current_info))
            exchange_file.close()

        logging.info(f"Updated markets info with {len(refactored_symbols)} symbols")
