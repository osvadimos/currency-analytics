import os
import pathlib
import uuid
import logging
from unittest import TestCase

from data_storage.aws.s3.S3Service import S3Service
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class TestS3Helper(TestCase):

    s3_helper = S3Service()
    test_directory = os.environ['LOCAL_STORAGE_ABSOLUTE_PATH']

    def setUp(self) -> None:
        #todo clean s3
        pass

    def test_random_string(self):
        random_string = TestS3Helper.create_random_string(10)
        print(random_string)
        self.assertTrue(len(random_string) == 10)
        #
        random_string = TestS3Helper.create_random_string()
        self.assertTrue(len(random_string) == 5)

    def test_is_object_exist(self):
        # todo create random string
        s3_test_key = "unit/test/object/" + TestS3Helper.create_random_string(10)
        # random part
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

    def test_sync_directories_to_s3(self):
        s3_service = S3Service()
        #todo create file in directory
        absolute_file = os.path.join( self.test_directory, TestS3Helper.create_random_string(10))
        random_file_content =  TestS3Helper.create_random_string(10)
        #todo file_content in file
        #todo write some string to a file
        s3_service.synchronize_directory(self.test_directory, is_local_to_s3=True)
        print(f'file:{absolute_file}')
        print(f"aws s3 ls {self.test_directory}")
        print(f"aws s3 cp {absolute_file} /tmp/tmp-file.txt")
        #todo check if object exists.
        #self.assertTrue(pathlib.Path(local_file_path).is_fifo())
        # в терминале "aws --profile clipnow s3 ls self.test_directory
        # в терминале "aws --profile clipnow s3 ls s3://bucket/absolute_file
        # в терминале "aws --profile clipnow s3 cp s3://bucket/absolute_file /tmp/local_file

    def test_sync_directories_from_s3(self):
        print("Some actions")
        self.assertTrue(pathlib.Path(self.test_directory))


    @staticmethod
    def create_random_string(str_length: int = 5) -> str:
        random_string = str(uuid.uuid4())
        return random_string[0:str_length]