import os

from data_storage.currency.Client import Client, CandlesticksChartIntervals


class CurrencyService:
    token = os.environ['CURRENCY_ACCESS']
    secret = os.environ['CURRENCY_SECRET']
    pass

    def __init__(self):
        self.currency_client = Client(self.token, self.secret)

    def pull_price_history(self, symbol,
                           intervals: CandlesticksChartIntervals):
        k_lines = self.currency_client.get_klines(symbol['symbol'],
                                                  intervals,
                                                  limit=1000)

        return k_lines

    def pull_depth(self, symbol):
        result = self.currency_client.get_order_book(symbol)
        return result

    def pull_exchange_info(self):
        result = self.currency_client.get_exchange_info()
        return result
