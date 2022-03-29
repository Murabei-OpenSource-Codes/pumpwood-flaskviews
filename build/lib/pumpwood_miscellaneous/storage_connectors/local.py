"""Set storage connector for local files."""
import os


class PumpWoodLocalBucket():
    def __init__(self, folder_path):
        self.folder_path = folder_path
        if not os.path.isdir(folder_path):
            template = 'Local bucket folder "{folder_path}" does not exist.'
            raise Exception(template.format(folder_path=folder_path))

    def write_file(self, file_path: str, data: bytes, if_exists: str = 'fail',
                   content_type='text/plain') -> str:
        if if_exists in ['overide', 'append_breakline', 'append', 'fail']:
            template = "if_exists must be in ['overide', " + \
                "'append_breakline', 'append', 'fail']"
            Exception(template)

        full_file_name = os.path.join(self.folder_path, file_path)

        folder = os.path.dirname(full_file_name)
        if not os.path.exists(folder):
            os.makedirs(folder)

        if os.path.isfile(full_file_name) and if_exists == 'fail':
            raise Exception('There is a file with same name on bucket')
        elif if_exists == 'append_breakline':
            with open(full_file_name, 'ab') as file:
                file.write(b'\n')
                file.write(data)
        elif if_exists == 'append':
            with open(full_file_name, 'ab') as file:
                file.write(data)
        else:
            with open(full_file_name, 'wb') as file:
                file.write(data)
        return file_path

    def delete_file(self, file_path: str):
        full_file_name = os.path.join(self.folder_path, file_path)

        if not os.path.isfile(full_file_name):
            Exception('file_path %s does not exist' % file_path)
        os.remove(full_file_name)
        return True

    def read_file(self, file_path):
        full_file_name = os.path.join(self.folder_path, file_path)
        if not os.path.isfile(full_file_name):
            Exception('file_path %s does not exist' % file_path)
        with open(full_file_name, 'rb') as file:
            data = file.read()
        return {'data': data, 'content_type': 'text/plain'}
