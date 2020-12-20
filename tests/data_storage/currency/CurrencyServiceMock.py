import os
import json

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
