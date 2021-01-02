import json
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd

from data_storage.currency.Client import CandlesticksChartIntervals
from data_storage.currency.CurrencyService import CurrencyService


class Symbol:
    local_storage_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']
    postfix = ".json"
    symbol_columns = ["open_time", "open", "high", "low", "close", "volume"]

    def __init__(self, symbol_info):
        self.name = symbol_info['name']
        self.id = symbol_info['symbol']
        self.symbol_data = pd.read_json(Symbol.symbol_data_file_path(symbol_info))
        self.symbol_information = symbol_info

    def process_symbol(self, currency_service: CurrencyService):

        if self.is_trading_open():
            logging.info(f"Start updating Symbol:{self.name}")
            self.upgrade_symbol_data(currency_service)
            existing_data_frame = self.symbol_data
            latest_record_date = existing_data_frame['open_time'].max()

            if not self.is_data_relevant():
                return

            # todo process algorithms for anomaly detecting

    @staticmethod
    def is_data_relevant(latest_record_date) -> bool:
        now_utc = datetime.now()
        hour_old = now_utc - timedelta(days=0,
                                       hours=1,
                                       minutes=0,
                                       seconds=0)
        return latest_record_date.replace(tzinfo=None) > hour_old.replace(tzinfo=None)

    @staticmethod
    def create_local_symbol(market_id: str, name: str, info: str, storage: str):
        market_information = {
            'market_id': market_id,
            'market_name': name,
            'market_info': info
        }
        path = Path(Symbol.symbol_data_file_path(market_id))
        if not os.path.exists(path.parent):
            os.makedirs(path.parent)

        market_file = open(str(path), "w")
        market_file.write(json.dumps(market_information))
        market_file.close()

        return Symbol(market_id)

    def upgrade_symbol_data(self, currency_service):

        logging.info(f"Start updating Symbol:{self.name}")
        records_path = self.symbol_data_file_path(self.symbol_information)
        if os.path.exists(records_path):
            existing_data_frame = pd.read_json(records_path)
        else:
            existing_data_frame = pd.DataFrame([], columns=self.symbol_columns)
        latest_record_date = existing_data_frame['open_time'].max()
        logging.info(f"Asset file for symbol:{self.id} : exists")
        currency_result = []
        for interval in [CandlesticksChartIntervals.MINUTE,
                         CandlesticksChartIntervals.FIVE_MINUTES,
                         CandlesticksChartIntervals.FIFTEEN_MINUTES,
                         CandlesticksChartIntervals.FOUR_HOURS,
                         CandlesticksChartIntervals.DAY,
                         CandlesticksChartIntervals.WEEK]:
            currency_request_result = currency_service.pull_price_history(self.id,
                                                                          interval,
                                                                          None)

            if len(currency_request_result) == 0:
                logging.info(f"Symbol:{self.name} is empty for some reason.")
                break
            currency_result.extend(currency_request_result)
            logging.info(f"Updating interval:{interval}, symbol:{self.id}")
            current_data_frame = pd.DataFrame(currency_result, columns=self.symbol_columns)
            if int(latest_record_date.timestamp() * 1000) > current_data_frame['open_time'].min():
                latest_records_data_frame = current_data_frame[
                    current_data_frame['open_time'] > int(latest_record_date.timestamp() * 1000)]
                existing_data_frame.append(latest_records_data_frame)
                logging.info(f"Found open time:{current_data_frame['open_time'].min()} and append to existing df")
                break
        if len(currency_result) == 0:
            logging.info(f"No data from server for symbol:{self.id}")
            return
        existing_data_frame.to_json(records_path)
        logging.info(f"Got out of loop on interval:{interval} and saved in file:{records_path}")

    @staticmethod
    def symbol_data_file_path(symbol_info) -> str:
        records_path = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'],
                                    f"{symbol_info['baseAsset']}-{symbol_info['quoteAsset']}{Symbol.postfix}")
        return records_path

    # todo create parsing time and test

    def is_trading_open(self) -> bool:
        # todo parse hours
        # todo covert server time
        trading_hours = self.symbol_information['tradingHours']
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
