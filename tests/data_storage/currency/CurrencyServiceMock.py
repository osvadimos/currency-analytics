import json
import os

from data_storage.currency.CurrencyService import CurrencyService


class CurrencyServiceMock(CurrencyService):

    def __init__(self):
        super().__init__()

    def pull_exchange_info(self):
        exchange_info_path = os.path.join(os.getcwd(), 'test_assets/exchange_info.json')
        with open(exchange_info_path, 'r') as exchange_file:
            exchange_info = exchange_file.read()
            exchange_file.close()
        result = json.loads(exchange_info)
        return result

    @staticmethod
    def generate_bitcoin_symbol():
        return {'symbol': 'BTC/USDT', 'name': 'Bitcoin / Tether', 'status': 'TRADING', 'baseAsset': 'BTC',
                'baseAssetPrecision': 3, 'quoteAsset': 'USDT', 'quoteAssetId': 'USDT', 'quotePrecision': 3,
                'orderTypes': ['LIMIT', 'MARKET'],
                'filters': [{'filterType': 'LOT_SIZE', 'minQty': '0.001', 'maxQty': '100', 'stepSize': '0.001'},
                            {'filterType': 'MIN_NOTIONAL', 'minNotional': '24'}], 'marketType': 'SPOT',
                'country': '',
                'sector': '', 'industry': '',
                'tradingHours': 'UTC; Mon - 22:00, 22:05 -; Tue - 22:00, 22:05 -; Wed - 22:00, 22:05 -; Thu - 22:00, 22:05 -; Fri - 22:00, 23:01 -; Sat - 22:00, 22:05 -; Sun - 21:00, 22:05 -',
                'tickSize': 0.01, 'tickValue': 235.3255, 'exchangeFee': 0.2}

    @staticmethod
    def pull_latest_bitcoin_data():
        bitcoin_dataframe = os.path.join(os.getcwd(), 'test_assets/BTC-USDT.json')
        with open(bitcoin_dataframe, "r") as bitcoin_file:
            bitcoin_data = bitcoin_file.read()
            bitcoin_file.close()
        return json.loads(bitcoin_data)



