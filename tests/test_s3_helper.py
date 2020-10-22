import pathlib
import os
from unittest import TestCase
from data_storage.s3_helper import S3Helper


class TestS3Helper(TestCase):

    s3_helper = S3Helper()
    test_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']

    def test_is_object_exist(self):
        # todo create random string
        s3_test_key = "unit/test/object/"  # random part
        # todo create tst object on s3
        self.s3_helper.save_object_on_s3(s3_test_key, "test body")

        self.assertTrue(self.s3_helper.is_object_exist(s3_test_key))
        # todo remove object
        self.assertTrue(not self.s3_helper.is_object_exist(s3_test_key))

    def test_read_object_and_save_as_file(self):
        # todo create random string
        s3_test_key = "unit/test/object/"  # random part
        # todo create tst object on s3
        self.s3_helper.save_object_on_s3(s3_test_key, "test body")
        local_file_path = self.test_directory + "/" + s3_test_key
        self.s3_helper.read_object_and_save_as_file(s3_test_key, local_file_path)
        self.assertTrue(pathlib.Path(local_file_path).is_fifo())

        # todo remove file
        os.remove(local_file_path)
