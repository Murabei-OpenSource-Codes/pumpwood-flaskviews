"""Functions and classes for flask/SQLAlchemy models."""
from flask_sqlalchemy import Model
from sqlalchemy import Column, BigInteger


class FlaskPumpWoodBaseModel(Model):
    """
    Flask Sqlalchemy Database Connection.

    - adds a id column for all models
    """

    id = Column(BigInteger, primary_key=True)
    '''All tables must have primary id'''
