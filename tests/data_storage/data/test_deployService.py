from unittest import TestCase

from data_storage.data.DeployService import DeployService
from tests.data_storage.currency.CurrencyServiceMock import CurrencyServiceMock


class TestDeployService(TestCase):

    def setUp(self) -> None:
        # todo clean storage before start
        pass

    def test_deploy_data_if_empty_storage(self):
        deploy_service = DeployService()
        deploy_service.currency_service = CurrencyServiceMock()
        # todo remove file from storage if eixsts

        deploy_service.deploy_data()

        # todo add file to storage if not exists

    def test_deploy_data_if_not_empty_storage(self):
        # todo add somfe files first and then check if they are changed or something.
        pass
