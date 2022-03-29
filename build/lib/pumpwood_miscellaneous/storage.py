"""PumpWood Storage Module."""
import io
import datetime
from werkzeug.utils import secure_filename
from pumpwood_flaskmisc.storage_connectors.google import PumpWoodGoogleBucket
from pumpwood_flaskmisc.storage_connectors.local import PumpWoodLocalBucket
from pumpwood_flaskmisc.storage_connectors.aws import PumpWoodAwsS3


def allowed_extension(filename, allowed_extensions,
                      exception: Exception = Exception):
    """Check if file have extension in allowed_extensions."""
    extension = 'none'
    if '.' in filename:
        extension = filename.rsplit('.', 1)[1].lower()

    if extension not in allowed_extensions:
        template = "File {filename} with extension {extension} not" + \
                   "allowed.\nAllowed extensions: {allowed_extensions}"
        raise exception(template.format(
            filename=filename, extension=extension,
            allowed_extensions=str(allowed_extensions)
        ))


class PumpWoodStorage():
    """Class to save midia files at PumpWood."""

    storage_object = None
    'Storage object'
    base_path = None
    'Path to be added to begin of the file'

    def __init__(self, storage_type: str = None, base_path: str = None, *args,
                 **kwargs):
        """Start the PumpWood storage class."""
        if storage_type is not None:
            self.base_path = base_path
            if storage_type == 'google_bucket':
                self.storage_object = PumpWoodGoogleBucket(
                    bucket_name=kwargs['bucket_name'])
            elif storage_type == 'aws_s3':
                self.storage_object = PumpWoodAwsS3(
                    bucket_name=kwargs['bucket_name'])
            elif storage_type == 'local':
                self.storage_object = PumpWoodLocalBucket(
                    folder_path=kwargs['folder_path'])
            else:
                raise Exception('Storage %s not implemented' % storage_type)

    def init(self, storage_type: str, base_path: str = None, *args, **kwargs):
        """Start the PumpWood storage class object."""
        if self.storage_object is None:
            self.__init__(storage_type, base_path, *args, **kwargs)
        else:
            raise Exception('storage_type already initialized.')

    def _update_file_path(self, file_path):
        return (self.base_path + '/' + file_path
                if self.base_path is not None else file_path)

    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if file exists.

        Args:
            file_path [str]: Path to file in storage.

        Return:
            Return a boolean value checking if the file exists on storage.
        """
        return self.storage_object.check_file_exists(
            file_path=file_path)

    def write_file(self, file_path: str, file_name: str, data: bytes,
                   unique_name: bool = False, if_exists: str = 'fail',
                   content_type='text/plain'):
        r"""
        Write a file to the storage.

        Args:
            file_path(str): Path which will be used to save files at storage
            data(bytes): File content.

        Kwargs:
            if_exists='fail'(str):
                fail to raise Exception if exists, append_breakline to append
                with a breakline between old content and new, append to
                append without breakline, overide to overide file.

            content_type='text/plain'(str): File content type.

        Returns:
            str: File name that was written.

        Raises:
            Exception('There is a file with same name on bucket'):
                if if_exists='fail' and there is a file with the same name.

        Example:
            >>> test = PumpWoodStorage(storage_type="google_bucket",
                                       bucket_name='my-bucket')
            >>> test.write_file(
                    'chubaca_eh_legal.txt',
                    data=b"dfjkasnfdkjsnkljsnlkjsn\ndkakjfnas\n",
                    if_exists='overide')

        """
        file_path = self._update_file_path(file_path)
        file_name = self._create_safe_filename(
            file_name=file_name, unique_name=unique_name)
        file_path = file_path + file_name
        return self.storage_object.write_file(
            file_path=file_path, data=data, if_exists=if_exists,
            content_type=content_type)

    def write_file_stream(self, file_path: str, file_name: str,
                          data_stream: io.BytesIO, unique_name: bool = False,
                          chunk_size: int = 1024 * 1024) -> dict:
        """
        Write file as a streaming process to storage.

        Args:
            file_path (str): Path to be used on file.
            file_name (str): Name of the file.
            data_stream (io.BytesIO): Data stream.
        Kwargs:
            unique_name (str): If date time will be used as sufix to make name
                unique.
            chunk_size (str): Chuck size of the streaming.
        """
        file_path = self._update_file_path(file_path)
        file_name = self._create_safe_filename(
            file_name=file_name, unique_name=unique_name)
        file_path = file_path + file_name
        return self.storage_object.write_file_stream(
            file_path=file_path, data_stream=data_stream,
            chunk_size=chunk_size)

    def delete_file(self, file_path: str):
        """
        Delete a file from storage.

        Args:
            file_path(str): Path which will be used to save files at storage

        Kwargs:
            No Kwargs.

        Returns:
            boolean: Only returns True

        Raises:
            Exception('file_path %s does not exist' % file_path):
                If file does not exists.

        Example:
            >>> test = PumpWoodStorage(storage_type="google_bucket",
                                       bucket_name='my-bucket')
            >>> test.delete_file('chubaca_eh_legal.txt')

        """
        return self.storage_object.delete_file(
            file_path=file_path)

    def read_file(self, file_path: str):
        """
        Read a file from storage.

        Args:
            file_path(str): File path.

        Kwargs:
            No Kwargs.

        Returns:
            bytes: File content

        Raises:
            Exception('file_path %s does not exist' % file_path):
                if file does not exists.

        Example:
            >>> test = PumpWoodStorage(storage_type="google_bucket",
                                       bucket_name='my-bucket')
            >>> test.read_file('chubaca_eh_legal.txt')

        """
        return self.storage_object.read_file(file_path=file_path)

    def download_to_file(self, file_path: str, file_obj):
        """
        Download cloud file to a file like object.

        Args:
            file_path (str): Cloud file path.
            file_obj (any): A file like object.
        Kwargs:
            No Kwargs
        Raises:
            No specific raises.
        """
        self.storage_object.download_to_file(
            file_path=file_path, file_obj=file_obj)

    def get_read_file_iterator(self, file_path):
        """
        Get an iterator to download file by chunks.

        Args:
            file_path(str): File path.

        Kwargs:
            No Kwargs.

        Returns:
            iterator: To loop over file chunks.

        Raises:
            Exception('file_path %s does not exist' % file_path):
                if file does not exists.

        Example:
            >>> test = PumpWoodStorage(storage_type="google_bucket",
                                       bucket_name='my-bucket')
            >>> test.read_file('chubaca_eh_legal.txt')

        """
        return self.storage_object.get_read_file_iterator(file_path=file_path)

    def _create_safe_filename(self, file_name: str,
                              unique_name: bool = False) -> str:
        """
        Create a safe filename including datetime to its name.

        Args:
            file_name(str): File path.

        Kwargs:
            unique_name (bool): Create an unique name for the file
                (add datetime to begining).

        Returns:
            str: New file name.

        Raises:
            No exceptions.

        Example:
            >>> test = PumpWoodStorage(storage_type="google_bucket",
                                       bucket_name='my-bucket')
            >>> test.create_safe_filename('chubaca_eh_legal.txt')

        """
        file_name = secure_filename(file_name)
        if not unique_name:
            return file_name
        else:
            date = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
            return "{0}___{1}".format(date, file_name)
