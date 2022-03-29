import requests
from unittest import TestCase
from pumpwood_flaskmisc.storage import PumpWoodGoogleBucket


class TestGoogleStorage(TestCase):
    """Test storage class."""

    def test_streming_upload_storage(self):
        storage_object = PumpWoodGoogleBucket(bucket_name="test-murabei")
        data_stream = open("db/test_upload.bin", "rb")

        storage_object.write_file_stream(
            file_path="pumpwood_flaskmisc/test_upload.bin",
            data_stream=data_stream)
        return storage_object

    def test_streming_download_storage(self):
        storage_object = self.test_streming_upload_storage()

        iterator = storage_object.get_read_file_iterator(
            file_path="pumpwood_flaskmisc/test_upload.bin")
        file_bytes = b''.join(iterator)

        with open("db/test_upload.bin", "rb") as file:
            original_data = file.read()
        self.assertEqual(file_bytes, original_data)
