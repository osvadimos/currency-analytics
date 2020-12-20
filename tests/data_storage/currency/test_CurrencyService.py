import json
from unittest import TestCase

from data_storage.currency.Client import CandlesticksChartIntervals
from data_storage.currency.CurrencyService import CurrencyService
from data_storage.data.DeployService import DeployService
from tests.data_storage.currency.CurrencyServiceMock import CurrencyServiceMock
import pandas as pd


class TestCurrencyService(TestCase):

    def test_depth(self):
        currency_service = CurrencyService()
        result = currency_service.pull_depth("BTC")
        result

    def test_pull_price_history(self):
        currency_service = CurrencyService()
        bitcoin = CurrencyServiceMock.generate_bitcoin_symbol()
        price_history = currency_service.pull_price_history(bitcoin,
                                                            CandlesticksChartIntervals.MINUTE,
                                                            None)
        js = json.dumps(price_history)
        js

    def test_pull_price_history_with_start(self):
        currency_service = CurrencyService()
        bitcoin = CurrencyServiceMock.generate_bitcoin_symbol()
        latest_data = CurrencyServiceMock.pull_latest_bitcoin_data()
        btc_dataframe = pd.DataFrame(latest_data, columns=DeployService.symbol_columns)
        first_record_time = btc_dataframe['open_time'].min()
        price_history = currency_service.pull_price_history(bitcoin,
                                                            CandlesticksChartIntervals.MINUTE,
                                                            first_record_time)
        price_history

    def test_exchange_inf(self):
        currency_service = CurrencyService()
        result = currency_service.pull_exchange_info()
        # todo save symbols as markets for further usage.
        # {'symbol': 'EVK', 'name': 'Evonik', 'status': 'BREAK', 'baseAsset': 'EVK', 'baseAssetPrecision': 3, 'quoteAsset': 'EUR', 'quoteAssetId': 'EUR', 'quotePrecision': 3, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '27000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'DE', 'sector': 'Basic Materials', 'industry': 'Diversified Chemicals', 'tradingHours': 'UTC; Mon 08:02 - 16:30; Tue 08:02 - 16:30; Wed 08:02 - 16:30; Thu 08:02 - 16:30; Fri 08:02 - 16:30', 'tickSize': 0.005, 'exchangeFee': 0.05}
        print(result['symbols'])
        import json
        result = json.dumps(result)
        result
