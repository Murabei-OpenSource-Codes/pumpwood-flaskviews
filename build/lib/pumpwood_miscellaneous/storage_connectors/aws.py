"""Google Storage Cloud."""
import io
import os
import boto3
import botocore
from typing import Callable


class PumpWoodAwsS3():
    """Class to make comunication with AWS S3 Storage."""

    def __init__(self, bucket_name: str, AWS_ACCESS_KEY_ID: str = None,
                 AWS_SECRET_ACCESS_KEY: str = None):
        """
        __init__.

        AWS credentials must be passed as arguments or set as enviroment
        variables: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY

        Args:
            bucket_name (str): Name of the bucket.
        Kwargs:
            AWS_ACCESS_KEY_ID [str]: Set Access key for AWS boto client.
            AWS_SECRET_ACCESS_KEY [str]: Set Secret Access key for AWS boto
                client.
        """
        if AWS_ACCESS_KEY_ID is None:
            AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        if AWS_SECRET_ACCESS_KEY is None:
            AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        if AWS_ACCESS_KEY_ID is None or AWS_SECRET_ACCESS_KEY is None:
            raise Exception(
                "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not passed as "
                "arguments and not set as enviroment variables")

        self._bucket_name = bucket_name
        self._s3_resource = boto3.client(
            's3', aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if file exists.

        Args:
            file_path [str]: Path to file in storage.

        Return:
            Return a boolean value checking if the file exists on storage.
        """
        try:
            self._s3_resource.head_object(
                Bucket=self._bucket_name, Key=file_path)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise e

    def write_file(self, file_path: str, data: bytes, if_exists: str = 'fail',
                   content_type='text/plain') -> str:
        """
        Write file on Google Bucket.

        Args:
            file_path (str): Path to save the file.
            data (str): File content in bytes.
            if_exists (str): if_exists must be in 'overide',
                'overide_streaming' (stream file to overide if exists),
                'append_breakline' (append content with a breakline between),
                'append' (append content without break line),
                'fail' (fail if file exists)]
            content_type (str): Mime-type of the content.
        """
        if_exists_opt = ['overide', 'append_breakline', 'append', 'fail']

        if if_exists in if_exists_opt:
            Exception("if_exists must be in {}".format(if_exists_opt))

        if self.check_file_exists(file_path=file_path):
            if if_exists == "fail":
                raise Exception('There is a file with same name on bucket')

            old_data = self.read_file(file_path=file_path)
            if if_exists == "append":
                new_data = old_data + data
                self._s3_resource.put_object(
                    Body=new_data, Bucket=self._bucket_name,
                    Key=file_path)
            if if_exists == 'append_breakline':
                new_data = old_data + b'\n' + data
                self._s3_resource.put_object(
                    Body=new_data, Bucket=self._bucket_name,
                    Key=file_path)
        else:
            self._s3_resource.put_object(
                Body=data, Bucket=self._bucket_name,
                Key=file_path)
        return file_path

    def write_file_stream(self, file_path: str, data_stream: io.BytesIO,
                          chunk_size: int = 1024 * 1024):
        """
        Write file as stream to google cloud.

        Args:
            file_path (str): Path to save the stream in Google Storage Bucket.
            data_stream (io.BytesIO): Data stream.
        Kwargs:
            chunk_size(int): Size of the chuck to be transmited, default for
                1024 * 1024 (1Mb).
        Return (dict):
            Return the file path used to save data ("file_path" key) and the
            total of bytes that were transmited.
        Raises:
            No particular raises at this function.
        """
        raise NotImplementedError(
            "write_file_stream not implemented for AWS S3")

    def get_read_file_iterator(self,  file_path: str,
                               chunk_size: int = 1024 * 1024) -> Callable:
        """
        Return an iterator to stream download data in flask.

        Args:
            file_path (str): Storage path.
        Kwargs:
            chunk_size (int): Chunk size in bytes, default to 1Mb.
        Raises:
            No specific raises.
        """
        raise NotImplementedError(
            "get_read_file_iterator not implemented for AWS S3")

    def delete_file(self, file_path: str) -> bool:
        """
        Delete file from s3.

        Args:
            file_path [str]: Path of the file to be deleted from s3.
        Return [bool]:
            Returns True
        Raise:
            Exception('file_path %s does not exist' % file_path):
            If file is not found on s3.
        """
        if not self.check_file_exists(file_path=file_path):
            raise Exception('file_path %s does not exist' % file_path)

        self._s3_resource.delete_object(
            Bucket=self._bucket_name, Key=file_path)
        return True

    def read_file(self, file_path: str):
        """
        Read file from S3.

        Returns a dictionary with the content_type and data in bytes.

        Args:
            file_path [str]: Path of the file to be read in s3.
        Return [dict]:
            A dictionary with:
            - data [bytes]: With the content of the file.
            - content_type [str]: Content type associated with the file.
        """
        try:
            head_data = self._s3_resource.head_object(
                Bucket=self._bucket_name, Key=file_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise Exception('file_path %s does not exist' % file_path)
            else:
                raise e

        file_stream = io.BytesIO()
        self._s3_resource.download_fileobj(
            Bucket=self._bucket_name, Key=file_path,
            Fileobj=file_stream)
        return {
            'data': file_stream.getvalue(),
            'content_type': head_data["ContentType"]}

    def download_to_file(self, file_path: str, file_obj):
        """
        Download file from storage and save it in a local path.

        Args:
            file_obj (any): A file like object or stream.
            local_path (str): Local path to save file.
        Kwargs:
            No Kwargs
        Raises:
            No specific raises.
        """
        self._s3_resource.download_fileobj(
            Bucket=self._bucket_name, Key=file_path,
            Fileobj=file_obj)
        file_obj.close()
