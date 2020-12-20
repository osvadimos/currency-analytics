from unittest import TestCase

from data_storage.currency.CurrencyService import CurrencyService


class TestCurrencyService(TestCase):

    def test_depth(self):
        currency_service = CurrencyService()
        result = currency_service.pull_depth("BTC")
        result

    def test_exchange_inf(self):
        currency_service = CurrencyService()
        result = currency_service.pull_exchange_info()
        # todo save symbols as markets for further usage.
        # {'symbol': 'EVK', 'name': 'Evonik', 'status': 'BREAK', 'baseAsset': 'EVK', 'baseAssetPrecision': 3, 'quoteAsset': 'EUR', 'quoteAssetId': 'EUR', 'quotePrecision': 3, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '27000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'DE', 'sector': 'Basic Materials', 'industry': 'Diversified Chemicals', 'tradingHours': 'UTC; Mon 08:02 - 16:30; Tue 08:02 - 16:30; Wed 08:02 - 16:30; Thu 08:02 - 16:30; Fri 08:02 - 16:30', 'tickSize': 0.005, 'exchangeFee': 0.05}
        print(result['symbols'])
        import json
        result = json.dumps(result)
        result
