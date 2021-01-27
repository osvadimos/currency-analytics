import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

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
        self.status = symbol_info['status']
        self.latest_record = None
        try:
            if os.path.exists(Symbol.symbol_data_file_path(symbol_info)):
                logging.info(f"File{Symbol.symbol_data_file_path(symbol_info)} exists.")
                self.symbol_data = pd.read_json(Symbol.symbol_data_file_path(symbol_info))
            else:
                self.symbol_data = pd.DataFrame([], columns=Symbol.symbol_columns)
        except:
            logging.error(f"Failed to open Symbol:{self.id}. Remove storage file.", exc_info=sys.exc_info())
            os.remove(Symbol.symbol_data_file_path(symbol_info))
        self.symbol_information = symbol_info

    def process_symbol(self, currency_service: CurrencyService):

        if self.status == 'TRADING':
            logging.info(f"Start updating Symbol:{self.name}")
            if not self.upgrade_symbol_data(currency_service):
                logging.info(f"Data has not changed since last time.")
                return

            # todo disable relevance for now
            # latest_record_date = self.symbol_data['open_time'].max()
            # if not self.is_data_relevant(latest_record_date):
            #    logging.info(f"Data hasn't changed since Symbol:{self.name}")
            #    return

            self.detect_symbol_anomalies()
        else:
            logging.info(f"Symbol:{self.id} is in {self.status} status. Skip processing.")

    @staticmethod
    def is_data_relevant(latest_record_date) -> bool:
        now_utc = datetime.now()
        hour_old = now_utc - timedelta(days=0,
                                       hours=1,
                                       minutes=0,
                                       seconds=0)
        logging.info(f"Relevance between dates Now:{now_utc} and hour ago:{hour_old}")
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

    def upgrade_symbol_data(self, currency_service) -> bool:

        logging.info(f"Start updating Symbol:{self.name}")

        existing_data_frame = self.symbol_data.copy(deep=True)
        latest_record_date = existing_data_frame[self.symbol_columns[0]].max()
        self.latest_record = self.symbol_data[
            self.symbol_data[self.symbol_columns[0]] == existing_data_frame[self.symbol_columns[0]].max()]
        logging.info(f"Asset file for symbol:{self.id}:exists with latest record:{self.latest_record}")
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
            current_data_frame[self.symbol_columns[0]] = pd.to_datetime(
                current_data_frame[self.symbol_columns[0]] * 1000000)
            logging.info(
                f"Symbol:{self.name} latest_record:{latest_record_date} current:{current_data_frame[self.symbol_columns[0]].max()}")

            if latest_record_date == current_data_frame[self.symbol_columns[0]].max():
                logging.info(
                    f"Symbol:{self.name} latest_record:{latest_record_date} current:{current_data_frame[self.symbol_columns[0]].max()} has not changed since last update.")
                break

            if latest_record_date > current_data_frame[self.symbol_columns[0]].min():
                latest_records_data_frame = current_data_frame[
                    current_data_frame[self.symbol_columns[0]] > latest_record_date]
                existing_data_frame = existing_data_frame.append(latest_records_data_frame)
                logging.info(
                    f"Found open time:{current_data_frame[self.symbol_columns[0]].min()} and append to existing df")
                break

        if len(currency_result) == 0:
            logging.info(f"No data from server for symbol:{self.id}")
            return False

        if existing_data_frame[self.symbol_columns[0]].max() != latest_record_date:
            logging.info(f"Got out of loop on interval:{interval} and dropping duplicates.")
            existing_data_frame = pd.DataFrame.drop_duplicates(existing_data_frame)
            self.update_symbol_data(existing_data_frame)
            logging.info(f"Upgraded data for symbol:{self.id}")
            return True
        else:
            logging.info(f"Pulled the same data symbol:{self.id} skip upgrading.")
            return False

    # todo test this method
    def update_symbol_data(self, update_data_frame):
        update_data_frame.reset_index(drop=True, inplace=True)
        update_data_frame.to_json(Symbol.symbol_data_file_path(self.symbol_information))
        self.symbol_data = update_data_frame
        logging.info(f"Saved in file:{Symbol.symbol_data_file_path(self.symbol_information)}")

    @staticmethod
    def symbol_data_file_path(symbol_info) -> str:
        records_path = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'],
                                    f"{symbol_info['baseAsset']}-{symbol_info['quoteAsset']}{Symbol.postfix}")
        return records_path

    # todo test
    def detect_symbol_anomalies(self) -> bool:

        if self.latest_record is None:
            logging.info(f"Could not find latest record for symbol:{self.id}")
            return False

        if self.symbol_data[self.symbol_columns[0]].max() != self.latest_record[self.symbol_columns[0]].max():
            logging.info(f"Start processing anomalies symbol:{self.id}")
            # todo process algorithms for anomaly detecting
            # todo define opening (find time gap between recent records)
            # todo define gap
            # todo define up/down slope
            return True

        logging.info(f"Last record and data are the same symbol:{self.id}")
        return False
