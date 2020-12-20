import json
import logging
import os

from data_storage.aws.s3.s3_helper import S3Helper
from data_storage.currency.Client import CandlesticksChartIntervals
from data_storage.currency.CurrencyService import CurrencyService


# todo add explicit logging
class DeployService:
    exchange_info_file_path = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], "exchange_info.json")

    def __init__(self):
        self.currency_service = CurrencyService()
        self.s3_helper = S3Helper()

    # todo test
    def deploy_data(self):
        # todo update from s3 first
        logging.info(f"Start sync with s3 s3")

        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=True)

        result_exchange_info = self.currency_service.pull_exchange_info()
        if not self.is_data_same(self.exchange_info_file_path, result_exchange_info):
            # todo update
            with open(self.exchange_info_file_path, 'w') as exchange_file:
                exchange_file.write(json.dumps(result_exchange_info))
                exchange_file.close()

        for symbol in result_exchange_info['symbols']:
            result = self.currency_service.pull_price_history(symbol,
                                                              CandlesticksChartIntervals.MINUTE)
            # todo find latest saved date
            # todo if not data
            result = self.currency_service.pull_price_history(symbol)
            result

        logging.info(f"Synchronized all updated data with s3")
        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=False)

    # todo test method
    @staticmethod
    def is_data_same(absolute_file_path, current_info) -> bool:
        if not os.path.exists(absolute_file_path):
            return False

        with open(absolute_file_path, 'r') as exchange_file:
            stored_data = exchange_file.read()
            exchange_file.close()
        return json.dumps(current_info) == stored_data
