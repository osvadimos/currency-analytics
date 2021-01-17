import json
import os
from datetime import datetime, timedelta
from unittest import TestCase

from data_storage.currency.CurrencyService import CurrencyService
from data_storage.symbol.Symbol import Symbol
from tests.data_storage.currency.CurrencyServiceMock import CurrencyServiceMock


class TestSymbol(TestCase):

    def test_create_symbol_object(self):
        symbol_information = self.generate_symbol('ETH/EUR')
        symbol = Symbol(symbol_information)

        self.assertTrue(symbol.symbol_information == symbol_information)

    def test_create_file_path(self):
        symbol_info = self.generate_symbol('ETH/EUR')
        file_path = Symbol.symbol_data_file_path(symbol_info)

        self.assertTrue(file_path, "'/home/ubuntu/projects/python/crypto/currency-analytics/file_storage/ETH-EUR.json'")

    def test_process_symbol(self):
        symbol = Symbol(self.generate_symbol('ETH/EUR'))
        currency_service = CurrencyService()
        result = symbol.process_symbol(currency_service)
        self.assertTrue(not result)

    def test_upgrade_data_for_symbol(self):
        btc_symbol_info = CurrencyServiceMock.generate_bitcoin_symbol()

        currency_service = CurrencyServiceMock()
        # todo remove file from storage if eixsts
        btc_symbol = Symbol(btc_symbol_info)
        btc_symbol.upgrade_symbol_data(currency_service)

    def test_is_data_relevant(self):
        now_utc = datetime.now()
        hour_old = now_utc - timedelta(days=0,
                                       hours=0,
                                       minutes=59,
                                       seconds=0)

        valid = Symbol.is_data_relevant(hour_old)
        self.assertTrue(valid)
        hour_old = now_utc - timedelta(days=0,
                                       hours=1,
                                       minutes=1,
                                       seconds=0)

        not_valid = Symbol.is_data_relevant(hour_old)
        self.assertFalse(not_valid)

    def test_detect_anomalies(self):
        symbol = Symbol(self.generate_symbol('ETH/EUR'))
        # todo define previous latst date date

        symbol.detect_symbol_anomalies()
        self.assertTrue(True)

    @staticmethod
    def generate_symbol(symbol_id: str):
        records_path = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], "exchange_info.json")
        with open(records_path) as json_file:
            exchange_info = json.load(json_file)
            json_file.close()
        generated_symbol = None
        for symbol in exchange_info['symbols']:
            if symbol['symbol'] == symbol_id:
                generated_symbol = symbol
                break
        return generated_symbol
