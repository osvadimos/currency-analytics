import logging
import os
import pandas as pd
from data_storage.aws.s3.S3Service import S3Service


class MarketDataProcessor:
    local_storage_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']

    def __init__(self, market_code: str):
        self.market_id: str = market_code
        self.df_market = None

    def pull_market_information(self):
        logging.info(f"Start pulling info for market:{self.market_id}")
        # todo pull info from s3
        # load market from local storage into dataframe
        storage_path: str = os.path.join(self.local_storage_directory,
                                         self.market_id,
                                         'info.json')
        s3_key = f'{self.market_id}/market.json'
        self.pull_from_s3(storage_path, s3_key)

        # todo pull info from API

    def update_market_information(self):
        #todo update market info from API
        self.df_market
        pass

    def pull_market_data(self):
        logging.info(f"Start pulling data for market:{self.market_id}")

        storage_path: str = os.path.join(self.local_storage_directory,
                                         self.market_id,
                                         'data.json')
        s3_key = f'{self.market_id}/data.json'
        self.pull_from_s3(storage_path, s3_key)
        # todo pull info from s3
        # load market from local storage into dataframe
        self.df_market = pd.read_json(storage_path)
        pass

    def pull_from_s3(self, storage_file_path, s3_key):
        if not os.path.isfile(storage_file_path):
            s3_helper = S3Service()
            logging.info(f"Download from s3:{s3_key}")
            s3_helper.read_object_and_save_as_file(s3_key,
                                                   storage_file_path)

