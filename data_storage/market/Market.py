import json
import os
from pathlib import Path


class Market:
    local_storage_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']
    postfix = ".json"

    def __init__(self, market_id: str):
        with open(Market.market_info_file_path(market_id)) as json_file:
            data = json.load(json_file)
            json_file.close()
        self.market_id = data['market_id']
        self.market_info = data['market_info']
        self.market_name = data['market_name']

    @staticmethod
    def create_market(market_id: str, name: str, info: str, storage: str):
        market_information = {
            'market_id': market_id,
            'market_name': name,
            'market_info': info
        }
        path = Path(Market.market_info_file_path(market_id))
        if not os.path.exists(path.parent):
            os.makedirs(path.parent)

        market_file = open(str(path), "w")
        market_file.write(json.dumps(market_information))
        market_file.close()

        return Market(market_id)

    @staticmethod
    def market_info_file_path(market_id) -> str:
        info_file = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'],
                                 market_id,
                                 market_id + Market.postfix)
        return info_file
