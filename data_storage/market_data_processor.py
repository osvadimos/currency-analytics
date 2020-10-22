import logging
import os
from typing import List


class MarketDataProcessor:

    storage_path = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']

    def __init__(self, market_code: str):
        self.market_code: str = market_code

    def pull_market_information(self):
        logging.info(f"Start pulling info for market:{self.market_code}")
        pass

    def pull_list_of_markets_for_update(self) -> List[str]:
        logging.info(f"Start pulling markets")

        # todo find local list in a file

        # todo pull from s3

        # todo compare

        # todo

        return []
