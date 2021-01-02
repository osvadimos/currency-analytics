import json
import logging
import os
from datetime import datetime, timezone, timedelta

import pandas as pd

from data_storage.aws.s3.S3Service import S3Service
from data_storage.currency.Client import CandlesticksChartIntervals
from data_storage.currency.CurrencyService import CurrencyService


# todo add explicit logging
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

        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=False)

        result_exchange_info = self.currency_service.pull_exchange_info()
        if not self.is_data_same(self.exchange_info_file_path, result_exchange_info):
            logging.info(f"Markets data is not the same.")
            self.update_symbols_data(self.exchange_info_file_path, result_exchange_info)
        else:
            logging.info(f"Markets has not changed since last time.")

        for symbol in result_exchange_info['symbols']:
            if self.is_trading_open(symbol['tradingHours']):
                logging.info(f"Trading for symbol:{symbol['name']} is open. Start updating.")
                self.upgrade_symbol_data(symbol)
            else:
                logging.info(f"Trading for symbol:{symbol['name']} is closed.")

        logging.info(f"Synchronized all updated data with s3")
        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=True)

    def upgrade_symbol_data(self, symbol):

        result = []
        logging.info(f"Start updating Symbol:{symbol['name']}")
        records_path = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'],
                                    f"{symbol['baseAsset']}-{symbol['quoteAsset']}.json")
        if os.path.exists(records_path):
            existing_data_frame = pd.read_json(records_path)
            latest_record_date = existing_data_frame['open_time'].max()
            logging.info(f"{symbol['baseAsset']}-{symbol['quoteAsset']}.json : exists")
            for interval in [CandlesticksChartIntervals.MINUTE,
                             CandlesticksChartIntervals.FIVE_MINUTES,
                             CandlesticksChartIntervals.FIFTEEN_MINUTES,
                             CandlesticksChartIntervals.FOUR_HOURS,
                             CandlesticksChartIntervals.DAY,
                             CandlesticksChartIntervals.WEEK]:
                currency_request_result = self.currency_service.pull_price_history(symbol,
                                                                                   interval,
                                                                                   None)

                if len(currency_request_result) == 0:
                    logging.info(f"Symbol:{symbol['name']} is empty for some reason.")
                    break
                result.extend(currency_request_result)
                logging.info(f"Updating interval:{interval}, symbol:{symbol['symbol']}")
                current_data_frame = pd.DataFrame(result, columns=self.symbol_columns)
                if int(latest_record_date.timestamp() * 1000) > current_data_frame['open_time'].min():
                    latest_records_data_frame = current_data_frame[
                        current_data_frame['open_time'] > int(latest_record_date.timestamp() * 1000)]
                    existing_data_frame.append(latest_records_data_frame)
                    logging.info(f"Found open time:{current_data_frame['open_time'].min()} and append to existing df")
                    break
            existing_data_frame.to_json(records_path)
            logging.info(f"Got out of loop on interval:{interval} and saved in file:{records_path}")
        else:

            for interval in [CandlesticksChartIntervals.MINUTE,
                             CandlesticksChartIntervals.FIVE_MINUTES,
                             CandlesticksChartIntervals.FIFTEEN_MINUTES,
                             CandlesticksChartIntervals.FOUR_HOURS,
                             CandlesticksChartIntervals.DAY,
                             CandlesticksChartIntervals.WEEK]:
                result.extend(self.currency_service.pull_price_history(symbol,
                                                                       interval,
                                                                       None))
                logging.info(f"so far:{len(result)}")
            data_frame = pd.DataFrame(result, columns=self.symbol_columns)
            data_frame.to_json(records_path)

        logging.info(f"Update for Symbol:{symbol['name']} is complete")

    #todo create parsing time and test
    @staticmethod
    def is_trading_open(trading_hours: str) -> bool:
        # todo parse hours
        # todo covert server time
        return True
        trading_days = trading_hours.split(';')
        list_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        assert "UTC" in trading_hours and list_days[0] in trading_days[1]
        now_utc = datetime.now(timezone.utc)
        week_start = now_utc - timedelta(days=now_utc.weekday(),
                                         hours=now_utc.hour,
                                         minutes=now_utc.minute,
                                         seconds=now_utc.second)

        for trading_day in trading_days:
            if trading_day.strip().split(' ')[0] in list_days:
                list_hours = trading_day.strip()[3:].split(',')
        now_utc.weekday()

        return True

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
