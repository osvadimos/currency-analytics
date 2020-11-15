import os

from data_storage.market.Market import MarketPair
from data_storage.currency.Client import Client, CandlesticksChartInervals


class CurrencyService:
    token = os.environ['CURRENCY_ACCESS']
    secret = os.environ['CURRENCY_SECRET']
    pass

    def __init__(self):
        self.currency_client = Client(self.token, self.secret)

    def pull_price_history(self, market_pair: MarketPair):
        klines = self.currency_client.get_klines(
            market_pair.get_pair_id(),
            CandlesticksChartInervals.DAY)
        print(klines)
        pass

    def pull_depth(self, symbol):
        result = self.currency_client.get_order_book(symbol)
        return result

    def pull_exchange_info(self):
        result = self.currency_client.get_exchange_info()
        return result