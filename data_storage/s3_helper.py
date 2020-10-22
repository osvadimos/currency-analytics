import boto3
import os
import logging


class S3Helper:
    s3_bucket = os.environ['S3_BUCKET']
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ['ACCESS'],
        aws_secret_access_key=os.environ['SECRET'],
        region_name=os.environ['REGION_NAME']
    )

    def __init__(self):
        pass

    def is_object_exist(self, s3_key: str) -> bool:
        logging.info(f"Start checking object:{s3_key}")

        return False

    def save_object_on_s3(self, s3_key: str, object_body: str):
        logging.info(f"Start saving object:{s3_key} ")
        obj = self.s3_client.Object(self.s3_bucket, s3_key)
        obj.put(Body=object_body)
        logging.info(f'Uploaded new file:{s3_key} to s3')

    def read_object_and_save_as_file(self, s3_key, file_absolute_path) -> str:
        obj = self.s3_client.Object(self.s3_bucket, s3_key)
        object_from_s3 = obj.get()['Body'].read().decode('utf-8')
        # todo save object locally
        return object_from_s3
