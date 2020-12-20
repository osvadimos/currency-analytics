from data_storage.aws.s3.S3Service import S3Service


class S3ServiceMock(S3Service):

    def synchronize_directory(self, local_directory, is_local_to_s3=True):
        return None
