from datetime import datetime, timedelta
from unittest import TestCase

from data_storage.currency.CurrencyService import CurrencyService
from data_storage.symbol.Symbol import Symbol
from tests.data_storage.currency.CurrencyServiceMock import CurrencyServiceMock


class TestSymbol(TestCase):

    def test_create_symbol_object(self):
        symbol_information = self.generate_symbol()
        symbol = Symbol(symbol_information)

        self.assertTrue(symbol.symbol_information == symbol_information)

    def test_create_file_path(self):
        symbol_info = self.generate_symbol()
        file_path = Symbol.symbol_data_file_path(symbol_info)

        self.assertTrue(file_path, "'/home/ubuntu/projects/python/crypto/currency-analytics/file_storage/ETH-EUR.json'")

    def test_is_trading_open(self):
        symbol = Symbol(self.generate_symbol())
        result = symbol.is_trading_open()
        self.assertTrue(not result)

    def test_process_symbol(self):
        symbol = Symbol(self.generate_symbol())
        currency_service = CurrencyService()
        result = symbol.process_symbol(currency_service)
        self.assertTrue(not result)

    def test_upgrade_data_for_symbol(self):
        btc_symbol_info = CurrencyServiceMock.generate_bitcoin_symbol()

        currency_service = CurrencyServiceMock()
        # todo remove file from storage if eixsts
        btc_symbol = Symbol(btc_symbol_info)
        btc_symbol.upgrade_symbol_data(currency_service)

    def test_is_trading_open(self):
        btc_symbol_info = CurrencyServiceMock.generate_bitcoin_symbol()
        btc_symbol = Symbol(btc_symbol_info)
        is_open = btc_symbol.is_trading_open()
        self.assertTrue(not is_open)

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

    @staticmethod
    def generate_symbol():
        symbol_data = {'symbol': 'ETH/EUR',
                       'name': 'Ethereum / EUR',
                       'status': 'TRADING',
                       'baseAsset': 'ETH',
                       'baseAssetPrecision': 2,
                       'quoteAsset': 'EUR',
                       'quoteAssetId': 'EUR',
                       'quotePrecision': 2,
                       'orderTypes': ['LIMIT', 'MARKET'],
                       'filters': [{'filterType': 'LOT_SIZE', 'minQty': '0.01', 'maxQty': '1000', 'stepSize': '0.01'},
                                   {'filterType': 'MIN_NOTIONAL', 'minNotional': '7'}],
                       'marketType': 'SPOT',
                       'country': '',
                       'sector': '',
                       'industry': '',
                       'tradingHours': 'UTC; Mon - 22:00, 22:05 -; Tue - 22:00, 22:05 -; Wed - 22:00, 22:05 -; Thu - 22:00, 22:05 -; Fri - 22:00, 23:01 -; Sat - 22:00, 22:05 -; Sun - 21:00, 22:05 -',
                       'tickSize': 0.01,
                       'tickValue': 6.20375,
                       'exchangeFee': 0.2}
        return symbol_data
