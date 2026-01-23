"""Module to help connection with SQLAlchemy and create sessions."""
import time
from loguru import logger
from flask import Flask
from sqlalchemy.sql import text
from sqlalchemy.exc import OperationalError


def get_session(db):
    """Deprected.

    Args:
        db:
            Database used on Flask SQLAlchemy connection.

    Returns:
        A SQLAlchemy session.
    """
    session = db.session
    return session


class PumpwoodDBGuard:
    """Class to help chacking if database if avaiable and roolback on errors.

    Class to set pre-request and post request hooks to check if database
    is avaiable and to roolback sessions if an error ocour.
    """

    def init(self, app: Flask, db) -> None:
        """Function to lazy init the object."""
        # Register the hooks on the app instance
        self.db = db
        app.before_request(self.before_request_hook)
        app.teardown_request(self.teardown_request_hook)

    def _check_db(self) -> None:
        """Check if database is avaiable."""
        self.db.session.execute(text("SELECT 1;"))

    def _regenerate_connections(self) -> None:
        """Regenerate application connections."""
        self.db.engine.dispose()
        self.db.session.rollback()

    def before_request_hook(self) -> None:
        """Function ran before each request."""
        retry_counter = 0
        max_retries = 5

        while True:
            try:
                self._check_db()
                break
            except OperationalError as e:
                if retry_counter < max_retries:
                    log_msg = (
                        "Operational error on pre-request database check, "
                        "retry [{retry_counter}]. Error message:"
                        "\n{error_msg}").format(
                            retry_counter=retry_counter,
                            error_msg=str(e))
                    logger.warning(log_msg)
                    retry_counter = retry_counter + 1

                    # Wait a litte before regen the connections
                    time.sleep(0.10)
                    self._regenerate_connections()
                else:
                    logger.exception("Database unavailable after max retries.")
                    raise e

    def teardown_request_hook(self, exception=None) -> None:
        """Function to run after the request."""
        if exception:
            try:
                self.db.session.rollback()
            except Exception:
                logger.exception("Error when rolling back the database.")
        self.db.session.remove()
