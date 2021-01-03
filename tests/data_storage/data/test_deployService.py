import json
import os
from unittest import TestCase

from data_storage.data.DeployService import DeployService
from tests.data_storage.aws.s3.S3ServiceMock import S3ServiceMock
from tests.data_storage.currency.CurrencyServiceMock import CurrencyServiceMock


class TestDeployService(TestCase):

    def setUp(self) -> None:
        # todo clean storage before start
        pass

    def test_deploy_data_if_empty_storage(self):
        deploy_service = DeployService()
        deploy_service.currency_service = CurrencyServiceMock()
        deploy_service.s3_helper = S3ServiceMock()
        # todo remove file from storage if eixsts

        deploy_service.deploy_data()

        # todo add file to storage if not exists

    def test_upgrade_data_for_symbol(self):
        btc_symbol = CurrencyServiceMock.generate_bitcoin_symbol()
        deploy_service = DeployService()
        deploy_service.currency_service = CurrencyServiceMock()
        deploy_service.s3_helper = S3ServiceMock()
        # todo remove file from storage if eixsts

        deploy_service.upgrade_symbol_data(btc_symbol)

    def test_is_trading_open(self):
        trading_hours = 'UTC; Mon - 22:00, 22:05 -; Tue - 22:00, 22:05 -; Wed - 22:00, 22:05 -; Thu - 22:00, 22:05 -; Fri - 22:00; Sun 22:05 -'
        result = DeployService.is_trading_open(trading_hours)
        self.assertTrue(not result)

    def test_update_old_file(self):
        # todo where little file is getting created on every test run
        old_file_path = "test_assets/little/exchange_info.json"
        bit_file_path = "test_assets/big/exchange_info.json"
        with open(bit_file_path, 'r') as big_file:
            data = big_file.read()
            big_file.close()

        DeployService.update_symbols_data(old_file_path, json.loads(data))
        self.assertTrue(True)

    def test_deploy_data_if_not_empty_storage(self):
        # todo add somfe files first and then check if they are changed or something.
        pass

    # test update method with symbols in all files
    def test_update_little_file(self):
        from tests.data_storage.test_s3_helper import TestS3Helper
        absolute_file = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], TestS3Helper.create_random_string(10) + ".json")
        absolute_file_opening = open(absolute_file, 'tw')
        with open("test_assets/little/exchange_info.json", 'r') as little_info:
            absolute_file_opening.write(little_info.read())
            little_info.close()
            absolute_file_opening.close()

        with open("test_assets/big/exchange_info.json", 'r') as big_info:
            big_data = big_info.read()
            big_data = json.loads(big_data)
            big_data["symbols"] = self.genarate_big_data()
            big_info.close()

        DeployService.update_symbols_data(absolute_file, big_data)

        with open(absolute_file, 'r') as last_info:
            last_data = last_info.read()
            json_data = json.loads(last_data)
            self.assertTrue(len(json_data["symbols"]) == len(big_data["symbols"]))

    # test update method without symbols in little file
    def test_update_little_file_without_in_little(self):
        from tests.data_storage.test_s3_helper import TestS3Helper
        absolute_file = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], TestS3Helper.create_random_string(10) + ".json")
        absolute_file_opening = open(absolute_file, 'tw')
        with open("test_assets/test_without_in_little/little/exchange_info.json", 'r') as little_info:
            absolute_file_opening.write(little_info.read())
            little_info.close()
            absolute_file_opening.close()

        with open("test_assets/test_without_in_little/big/exchange_info.json", 'r') as big_info:
            big_data = big_info.read()
            big_data = json.loads(big_data)
            big_info.close()

        DeployService.update_symbols_data(absolute_file, big_data)

        with open(absolute_file, 'r') as last_info:
            last_data = last_info.read()
            json_data = json.loads(last_data)
            self.assertTrue(len(json_data["symbols"]) == len(big_data["symbols"]))

    # test update method without symbols in big file
    def test_update_little_file_without_in_big(self):
        from tests.data_storage.test_s3_helper import TestS3Helper
        absolute_file = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], TestS3Helper.create_random_string(10) + ".json")
        absolute_file_opening = open(absolute_file, 'tw')
        with open("test_assets/test_without_in_big/little/exchange_info.json", 'r') as little_info:
            absolute_file_opening.write(little_info.read())
            little_info.close()
            absolute_file_opening.close()

        with open("test_assets/test_without_in_big/big/exchange_info.json", 'r') as big_info:
            big_data = big_info.read()
            big_data = json.loads(big_data)
            big_info.close()

        DeployService.update_symbols_data(absolute_file, big_data)

        with open(absolute_file, 'r') as last_info:
            last_data = last_info.read()
            json_data = json.loads(last_data)
            self.assertTrue(len(json_data["symbols"]) == len(big_data["symbols"]))

    # test update method with symbols in all files and new value in big
    def test_update_little_file_without_in_all_file(self):
        from tests.data_storage.test_s3_helper import TestS3Helper
        absolute_file = os.path.join(os.environ['LOCAL_STORAGE_ABSOLUTE_PATH'], TestS3Helper.create_random_string(10) + ".json")
        absolute_file_opening = open(absolute_file, 'tw')
        with open("test_assets/test_in_all_file/little/exchange_info.json", 'r') as little_info:
            absolute_file_opening.write(little_info.read())
            little_info.close()
            absolute_file_opening.close()

        with open("test_assets/test_in_all_file/big/exchange_info.json", 'r') as big_info:
            big_data = big_info.read()
            big_data = json.loads(big_data)
            big_info.close()

        DeployService.update_symbols_data(absolute_file, big_data)

        with open(absolute_file, 'r') as last_info:
            last_data = last_info.read()
            json_data = json.loads(last_data)
            self.assertTrue(len(json_data["symbols"]) == len(big_data["symbols"]))

    @staticmethod
    def genarate_big_data():
        return [{'symbol': 'EVK', 'name': 'Evonik', 'status': 'BREAK', 'baseAsset': 'EVK', 'baseAssetPrecision': 3, 'quoteAsset': 'EUR', 'quoteAssetId': 'EUR', 'quotePrecision': 3, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '27000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'DE', 'sector': 'Basic Materials', 'industry': 'Diversified Chemicals', 'tradingHours': 'UTC; Mon 08:02 - 16:30; Tue 08:02 - 16:30; Wed 08:02 - 16:30; Thu 08:02 - 16:30; Fri 08:02 - 16:30', 'tickSize': 0.005, 'exchangeFee': 0.05}, {'symbol': 'MPW', 'name': 'Medical Properties', 'status': 'BREAK', 'baseAsset': 'MPW', 'baseAssetPrecision': 2, 'quoteAsset': 'USD', 'quoteAssetId': 'USD', 'quotePrecision': 2, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '34000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'US', 'sector': 'Financials', 'industry': 'Specialized REITs', 'tradingHours': 'UTC; Mon 14:30 - 21:00; Tue 14:30 - 21:00; Wed 14:30 - 21:00; Thu 14:30 - 21:00; Fri 14:30 - 21:00', 'tickSize': 0.01, 'exchangeFee': 0.05}, {'symbol': 'GBP/CAD', 'name': 'GBP/CAD', 'status': 'HALT', 'baseAsset': 'GBP', 'baseAssetPrecision': 5, 'quoteAsset': 'CAD', 'quoteAssetId': 'CAD', 'quotePrecision': 5, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '100', 'maxQty': '10000000', 'stepSize': '100'}], 'marketType': 'SPOT', 'country': '', 'sector': '', 'industry': '', 'tradingHours': 'UTC; Mon - 22:00, 22:05 -; Tue - 22:00, 22:05 -; Wed - 22:00, 22:05 -; Thu - 22:00, 22:05 -; Fri - 22:00; Sun 22:05 -', 'tickSize': 5e-05, 'exchangeFee': 1}, {'symbol': 'UPS', 'name': 'United Parcel', 'status': 'BREAK', 'baseAsset': 'UPS', 'baseAssetPrecision': 2, 'quoteAsset': 'USD', 'quoteAssetId': 'USD', 'quotePrecision': 2, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '11000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'US', 'sector': 'Industrials', 'industry': 'Air Freight & Courier Services', 'tradingHours': 'UTC; Mon 14:30 - 21:00; Tue 14:30 - 21:00; Wed 14:30 - 21:00; Thu 14:30 - 21:00; Fri 14:30 - 21:00', 'tickSize': 0.01, 'exchangeFee': 0.05}, {'symbol': 'APH', 'name': 'Amphenol', 'status': 'BREAK', 'baseAsset': 'APH', 'baseAssetPrecision': 2, 'quoteAsset': 'USD', 'quoteAssetId': 'USD', 'quotePrecision': 2, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '9000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'US', 'sector': '', 'industry': '', 'tradingHours': 'UTC; Mon 14:30 - 21:00; Tue 14:30 - 21:00; Wed 14:30 - 21:00; Thu 14:30 - 21:00; Fri 14:30 - 21:00', 'tickSize': 0.01, 'exchangeFee': 0.05}, {'symbol': 'APD', 'name': 'Air Products', 'status': 'BREAK', 'baseAsset': 'APD', 'baseAssetPrecision': 2, 'quoteAsset': 'USD', 'quoteAssetId': 'USD', 'quotePrecision': 2, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '8000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'US', 'sector': 'Basic Materials', 'industry': 'Commodity Chemicals', 'tradingHours': 'UTC; Mon 14:30 - 21:00; Tue 14:30 - 21:00; Wed 14:30 - 21:00; Thu 14:30 - 21:00; Fri 14:30 - 21:00', 'tickSize': 0.01, 'exchangeFee': 0.05}, {'symbol': 'EPD', 'name': 'Enterprise Products', 'status': 'BREAK', 'baseAsset': 'EPD', 'baseAssetPrecision': 2, 'quoteAsset': 'USD', 'quoteAssetId': 'USD', 'quotePrecision': 2, 'orderTypes': ['LIMIT', 'MARKET'], 'filters': [{'filterType': 'LOT_SIZE', 'minQty': '1', 'maxQty': '44000', 'stepSize': '1'}], 'marketType': 'SPOT', 'country': 'US', 'sector': 'Energy', 'industry': 'Oil & Gas Transportation Services', 'tradingHours': 'UTC; Mon 14:30 - 21:00; Tue 14:30 - 21:00; Wed 14:30 - 21:00; Thu 14:30 - 21:00; Fri 14:30 - 21:00', 'tickSize': 0.01, 'exchangeFee': 0.05}]