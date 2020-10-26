import os
import pandas as pd
import logging
from typing import List
from data_storage.s3_helper import S3Helper
from data_storage.market_data_processor import MarketDataProcessor


class MarketManager:
    local_storage_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']
    local_storage_file_name = 'market_manager_file.json'
    storage_s3_key = 'market_storage/market_manager_file.json'

    def __init__(self):
        # load market from local storage into dataframe
        self.market_storage_path: str = os.path.join(self.local_storage_directory, self.local_storage_file_name)
        self.s3_helper = S3Helper()
        if not os.path.isfile(self.market_storage_path):
            logging.info(f"Download from s3:{self.storage_s3_key}")
            self.s3_helper.read_object_and_save_as_file(self.storage_s3_key,
                                                        self.market_storage_path)

        self.pd_market_storage = pd.read_json(self.market_storage_path)

    def add_market(self, market_id: str, market_name: str, market_info: str):
        logging.info(f"Adding market:{market_id} name:{market_name}")
        self.pd_market_storage.loc[len(self.pd_market_storage)] = [market_id,
                                                                   market_name,
                                                                   market_info]
        self.pd_market_storage.to_json(self.market_storage_path)
        self.s3_helper.save_object_on_s3(self.storage_s3_key, self.pd_market_storage.to_json())

    def pull_list_of_markets_for_update(self) -> List[str]:
        logging.info(f"Start pulling markets")
        for _, row in self.pd_market_storage.iterrows():
            market_processor = MarketDataProcessor(row['market_id'])
            market_processor.pull_market_information()

        return []
