"""Functions and classes for flask/SQLAlchemy models."""
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, BigInteger
from flask_sqlalchemy.query import Query
from pumpwood_flaskviews.query import BaseQueryABC, BaseQueryNoFilter
from pumpwood_flaskviews.query import SqlalchemyQueryMisc
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.exceptions import PumpWoodObjectDoesNotExist
# from pumpwood_flaskviews.sqlalchemy import get_session


class FlaskPumpWoodBaseModel(DeclarativeBase):
    """Flask Sqlalchemy Database Connection.

    - adds a id column for all models
    """

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    """All tables must have primary id"""

    base_query: BaseQueryABC = BaseQueryNoFilter()
    """A base query object from `BaseQueryABC` class.

    `BaseQueryNoFilter` will not apply any filters.

    This object will set all end-points default filter of objects, it
    will be applied to all end-points (retrieve, list, delete, action, ...).
    It can be used to retrict access to data according to user row_permission
    or object ownership."""

    @classmethod
    def default_filter_query(cls, query: Query = None) -> Query:
        """Create a query with defailt filter.

        Use base query object to add default filter to objects. Base query
        object might use auth_header.
        """
        return cls.base_query.add_filter(model=cls, query=query)

    @classmethod
    def default_query_list(cls, filter_dict: None | dict = None,
                           exclude_dict: dict = None,
                           order_by: list = None, limit: int = None,
                           base_query: Query = None) -> Query:
        """Create a list query using parameter and appling default filters.

        Args:
            filter_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by (list):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            limit (int):
                Number of objects to be returned.
            base_query (Query):
                A base query to be used as initial filter.

        Returns:
            Results of query.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by

        base_query = cls.default_filter_query()
        query_result = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=cls,
                base_query=base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict,
                order_by=order_by)
        if limit is None:
            return query_result.all()
        else:
            return query_result.limit(limit).all()

    @classmethod
    def default_query_get(cls, pk: str | int,
                          base_query: Query = None,
                          raise_error: bool = True) -> object:
        """Get model_class object using pumpwood pk.

        Pumpwood pk may be integers and base64 strings coding a dictionary
        with composite primary keys. This function abstract SQLAlchemy
        query.get to treat both possibilities.

        Args:
            pk (str, int):
                Pumpwood primary key.
            base_query (Query):
                A base query to be used as initial filter.
            raise_error (bool):
                Raise error if object was not found.

        Return:
            Returns a SQLAlchemy object with corresponding primary key.
        """
        converted_pk = CompositePkBase64Converter.load(pk)
        if isinstance(converted_pk, (int, float)):
            # If a numeric data is passed as pk it is associated with
            # 'id' field, it is necessary to convert to a dict to unpack
            # on filter_by
            converted_pk = {'id': converted_pk}

        # Use base query to filter object acording to user's permission
        tmp_base_query = cls.default_filter_query(query=base_query)

        # Since base query inject a filter retricting user information
        # it is not possible to use .get
        model_object = tmp_base_query\
            .filter_by(**converted_pk).one()

        if model_object is None and raise_error:
            message = "Requested object {model_class}[{pk}] not found."
            raise PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": cls.__name__, "pk": pk})
        return model_object
