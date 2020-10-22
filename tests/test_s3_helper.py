from unittest import TestCase
from data_storage.s3_helper import S3Helper


class TestS3Helper(TestCase):

    s3_helper = S3Helper()

    def test_is_object_exist(self):

        # todo create random string
        s3_test_key = "unit/test/object/"  # random part
        # todo create tst object on s3
        self.s3_helper.save_object_on_s3(s3_test_key, "test body")

        self.assertTrue(self.s3_helper.is_object_exist(s3_test_key))
        # todo remove object
        self.assertTrue(not self.s3_helper.is_object_exist(s3_test_key))


    def test_read_object_and_save_as_file(self):
        self.fail()
