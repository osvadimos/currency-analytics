from unittest import TestCase
from data_storage.market_manager_service import SymbolManager


class TestMarketManager(TestCase):

    def test_process_symbols(self):
        market_manager = SymbolManager()
        market_id = 'BTC'
        market_name = 'BTC.dx'
        market_info = 'Bitcoin'

        # todo test if exists in pd
        # todo test if exists on s3
        # todo test if exists locally in file
