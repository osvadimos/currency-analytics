from unittest import TestCase
from data_storage.market_manager_service import MarketManager


class TestMarketManager(TestCase):

    def test_add_market(self):
        market_manager = MarketManager()
        # todo add randomity
        market_manager.storage_s3_key = "test/storage/key.json"
        market_manager.local_storage_file_name = "key.json"
        market_id = 'market_id'
        market_name = 'market_name'
        market_info = 'market_info'
        market_manager.add_market(market_id, market_name, market_info)
        # todo test if exists in pd
        # todo test if exists on s3
        # todo test if exists locally in file
