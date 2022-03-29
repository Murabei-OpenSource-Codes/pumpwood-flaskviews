"""RabbitMQ comunication module."""
import pika
import simplejson as json
from pumpwood_communication.serializers import PumpWoodJSONEncoder
from pumpwood_communication.exceptions import PumpWoodException


class PumpWoodRabbitMQ:
    """Comunication with RabbitMQ."""

    queue = None

    def __init__(self, queue: str = None, username: str = None,
                 password: str = None, host: str = None, port: int = None):
        """
        Start PumpWood RabbitMQ connection.

        Args:
            queue(str): Name of queue that will used in the service.
            username(str): Username to be used in the connection.
            password(str): Password to be used in the connection.
            host(str): Host to be used in the connection.
            port(int): Port yo be used in the connection.

        Kwargs:
            No extra fields.

        Returns:
            str: File name that was written.

        Raises:
            No exceptions.

        Example:
            >>> test = PumpWoodRabbitMQ(queue='teste', username='guest'
                , password='guest', host='localhost', port='')

        """
        if queue is not None:
            self.queue = queue

        self._username = username
        self._password = password
        self._host = host
        self._port = port

    def __repr__(self):
        """__repr__."""
        return '<PumpWoodRabbitMQ: queue=%s>' % (self.queue, )

    def init(self, queue: str, username: str, password: str, host: str,
             port: int):
        """Posterior object initiation."""
        self.__init__(queue, username, password, host, port)

    def send(self, data: any, queue: str = None) -> None:
        """
        Send RabitMQ a msg.

        Args:
            data(any): Data to be sent to RabitMQ.
        Kwargs:
            queue(str): An different queue to send data.
        Returns:
            str: File name that was written.

        Raises:
            No exceptions.

        Example:
            >>> test = PumpWoodRabbitMQ(queue='teste', username='guest'
                , password='guest', host='localhost', port='')

        """
        if queue is None and self.queue is None:
            raise PumpWoodException(
                message=(
                    "queue argment is None and queue not set at"
                    "constructor."))
        credentials = pika.PlainCredentials(self._username, self._password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self._host, credentials=credentials,
                port=self._port))

        queue = queue or self.queue
        channel = connection.channel()
        channel.queue_declare(queue=queue)
        channel.basic_publish(
            exchange='', routing_key=queue, body=json.dumps(
                data, cls=PumpWoodJSONEncoder, ignore_nan=True))
