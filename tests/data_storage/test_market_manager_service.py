from unittest import TestCase
from data_storage.market_manager_service import MarketManager


class TestMarketManager(TestCase):

    def test_add_market(self):
        market_manager = MarketManager()
        market_id = 'BTC'
        market_name = 'BTC.dx'
        market_info = 'Bitcoin'
        market_manager.add_market(market_id, market_name, market_info)
        # todo test if exists in pd
        # todo test if exists on s3
        # todo test if exists locally in file
