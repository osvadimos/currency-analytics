import logging
import os
from typing import List

from data_storage.aws.s3.S3Service import S3Service
from data_storage.currency.CurrencyService import CurrencyService
from data_storage.data.DeployService import DeployService
from data_storage.market.Market import Market
from data_storage.market_data_processor import MarketDataProcessor


class MarketManager:
    local_storage_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']
    s3_helper = S3Service()
    currency_service = CurrencyService()
    markets = []

    def list_markets(self) -> List[Market]:
        logging.info(f"Starting list markets")

        for market_ids in os.walk(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']):
            for market_id in market_ids[1]:
                logging.info(f"Found {market_id} directory")
                if os.path.isfile(Market.market_info_file_path(market_id)):
                    market = Market(str(market_id))
                    self.markets.append(market)
        logging.info(f"Found {len(self.markets)} markets")
        return self.markets

    def add_market(self, market_id: str, market_name: str, market_info: str):
        logging.info(f"Adding market:{market_id} name:{market_name}")
        market = Market.create_market(market_id,
                                      market_name,
                                      market_info,
                                      self.local_storage_directory)
        # todo pull information
        self.s3_helper.synchronize_directory(self.local_storage_directory,
                                             is_local_to_s3=True)

    def pull_list_of_markets_for_update(self) -> List[str]:
        logging.info(f"Start pulling markets")
        for _, row in self.pd_market_storage.iterrows():
            market_processor = MarketDataProcessor(row['market_id'])
            market_processor.pull_market_information()

        return []

    def process_markets(self):
        self.s3_helper.synchronize_directory(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], is_local_to_s3=False)
        logging.info(f"Synced markets with s3 and start sync with platform.")
        # todo find all markets
        result_exchange_info = self.currency_service.pull_exchange_info()
        # todo run update info script
        for symbol in result_exchange_info['symbols']:
            if DeployService.is_trading_open(symbol['tradingHours']):
                print(f"Process symbol")
                print(symbol)
                pass
        pass
