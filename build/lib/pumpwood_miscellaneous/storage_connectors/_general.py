"""Define general functions and class to interact with storage clients"""


class FlaskStreamUploadWrapper():
    """Wraps flask stream response to behave like a file object."""

    def __init__(self, flask_stream):
        self.bytes_position = 0
        self.stream = flask_stream

    def read(self, chunk_size: int) -> bytes:
        data = self.stream.read(chunk_size)
        self.bytes_position += len(data)
        return data

    def tell(self) -> int:
        return self.bytes_position


class FlaskStreamDownloadWrapper():
    """Wraps Download streaming to get last downloaded chunk."""

    def __init__(self):
        self.bytes_position = 0
        self.last_chuck = b''

    def write(self, data: bytes):
        self.last_chuck = data
        self.bytes_position += len(data)

    def tell(self) -> int:
        return self.bytes_position

    def get_last_chunk(self):
        """Get last downloaded chunk."""
        return self.last_chuck
