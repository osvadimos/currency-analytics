import logging
import os

import boto3


class S3Service:
    s3_bucket = os.environ['S3_BUCKET']
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ['ACCESS'],
        aws_secret_access_key=os.environ['SECRET'],
        region_name=os.environ['REGION_NAME']
    )

    def __init__(self):
        assert self.s3_bucket
        assert self.s3_client

    def is_object_exist(self, s3_key: str) -> bool:
        logging.info(f'Start checking object: {s3_key}')
        response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket,
                                                  Prefix=s3_key,
                                                  Delimiter='/')
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'] == s3_key:
                    return True
        return False

    def save_object_on_s3(self, s3_key: str, object_body: str):
        logging.info(f"Start saving object:{s3_key} ")
        obj = self.s3_client.Object(self.s3_bucket, s3_key)
        obj.put(Body=object_body)
        logging.info(f'Uploaded new file:{s3_key} to s3')

    def read_s3_object_as_string(self, s3_key) -> str:
        obj = self.s3_client.Object(self.s3_bucket, s3_key)
        object_from_s3 = obj.get()['Body'].read().decode('utf-8')
        # todo save object locally
        return object_from_s3

    def read_object_and_save_as_file(self, s3_key, file_absolute_path) -> str:
        obj = self.s3_client.Object(self.s3_bucket, s3_key)
        object_from_s3 = obj.get()['Body'].read().decode('utf-8')
        # todo save object locally
        return object_from_s3

    def list_s3_objects(self, prefix: str):
        # todo add for over 1k files
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
        list_of_objects = []
        logging.info('list of objects: ', list_of_objects)
        for page in pages:
            for obj in page['Contents']:
                list_of_objects.append(obj['Key'])
                logging.info(f'Added object {obj["Key"]} to list of objects')
        return list_of_objects

    #todo create test
    def synchronize_directory(self, local_directory, is_local_to_s3=True):
        s3_directory = local_directory.replace(os.environ['HOME'], "")
        s3_path = f's3://{os.path.join(self.s3_bucket, *s3_directory.split(os.sep))}'
        logging.info(f'Synchronisation of local:{local_directory} with destination:{s3_directory}')
        location = local_directory if is_local_to_s3 else s3_path
        destination = local_directory if not is_local_to_s3 else s3_path
        cmd = f'aws --profile crypto s3 sync {location}/ {destination} '
        os.system(cmd)
        logging.info(f'Synced well of local:{local_directory} with destination:{s3_directory}')
