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
