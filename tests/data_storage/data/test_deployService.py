import json
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
