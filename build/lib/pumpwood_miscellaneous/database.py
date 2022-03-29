# -*- coding: utf-8 -*-
"""Database helper functions module."""
from flask_sqlalchemy import SQLAlchemy
from pumpwood_communication.serializers import pumpJsonDump


def build_engine_string(dialect: str, database: str, driver: str = None,
                        username: str = None, password: str = None,
                        host: str = None, port: str = None, ssl: str = None):
    """
    Build SQLAlchemy engine string acordind to database parameters.

    Args:
        dialect (str): Dialect string
        database (str): Database name or path for SQLite
    Kwargs:
        driver (str): Database driver to be used
        username (str): Database username
        password (str): Database password
        host (str): Database host
        port (str): Database port

    Raises:
        Exception:
            Raise Exception if username or password or host or port are not
            set for a database that isn't Sqlite.

    """
    if dialect != 'sqlite':
        if username is None or password is None or host is None or\
                port is None:
            template = "Except for sqlite database for all others username," +\
                " password, host and port must be suplied:\n" + \
                "\nusername: {username};\npassword: {password}" + \
                ";\nhost: {host};\nport: {port};"

            raise_msg = template.format(
                username=username, password=password,
                host=host, port=port,)

            raise Exception(raise_msg)

    dialect_text = dialect
    driver_text = '+' + driver if driver is not None else ''
    username_text = username
    password_text = ':' + password if password is not None else ''
    host_text = '@' + host if host is not None else ''
    port_text = ':' + port if port is not None else ''
    database_text = database

    template = '{dialect}{driver}://{username}{password}{host}{port}' +\
        '/{database}'
    temp_string = template.format(
        dialect=dialect_text, driver=driver_text, username=username_text,
        password=password_text, host=host_text, port=port_text,
        database=database_text)

    if ssl is not None:
        temp_string = temp_string + "?sslmode=" + ssl
    return temp_string


class SQLAlchemyPostGres(SQLAlchemy):
    """Inicialize SQLAlchemy with a few tricks for PostGres."""
    def apply_driver_hacks(self, app, info, options):
        options.update({
            "pool_pre_ping": True, "json_serializer": pumpJsonDump})
        super(SQLAlchemyPostGres, self).apply_driver_hacks(app, info, options)
