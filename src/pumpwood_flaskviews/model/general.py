"""Functions and classes for flask/SQLAlchemy models."""
from loguru import logger
from flask import g
from typing import Literal
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, BigInteger
from flask_sqlalchemy.query import Query
from pumpwood_flaskviews.query import (
    BaseQueryABC, BaseQueryNoFilter, SqlalchemyQueryMisc)
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.exceptions import PumpWoodObjectDoesNotExist
from pumpwood_communication.cache import default_cache
# from pumpwood_flaskviews.sqlalchemy import get_session


def _try_convert_int(value: str):
    """Helper function to set type of the pk at error payload."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


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
    def build_get_cache_hash(cls, pk: str | int,
                             get_type: Literal['default', 'query']
                             ) -> str:
        """Build get hash dict.

        Args:
             pk (str | int):
                Primary to fetch the data.
             get_type (Literal('default', 'query')):
                Type of the request that is been made.

        Returns:
            Return a dictionary with hash dict to cache get.
        """
        hash_dict = AuthFactory.get_auth_header()
        hash_dict = {
            'context': 'flaskviews--model-query-retrieve',
            'model_class': cls.__name__,
            'pk': pk,
            'get-type': get_type}
        return default_cache._generate_hash(hash_dict)

    @classmethod
    def default_filter_query(cls, query: Query = None,
        filter_dict: dict = None, exclude_dict: dict = None,
        order_by: list[str] = None) -> Query:
        """Create a query with defailt filter.

        Use base query object to add default filter to objects. Base query
        object might use auth_header.

        Args:
            query (Query):
                Query used as starting point.
            filter_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by (list):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
        """
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by

        return cls.base_query.add_filter(
            model=cls, query=query, filter_dict=filter_dict,
            exclude_dict=exclude_dict, order_by=order_by)

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

        tmp_base_query = cls.default_filter_query(
            query=base_query, filter_dict=filter_dict,
            exclude_dict=exclude_dict, order_by=order_by)
        query_result = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=cls,
                base_query=tmp_base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict,
                order_by=order_by)
        if limit is None:
            return query_result
        else:
            return query_result.limit(limit)

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
            use_cache (bool):
                If local cache may be used to retrieve data. Is base query
                if not None, cache can not be used.

        Return:
            Returns a SQLAlchemy object with corresponding primary key.
        """
        hash_str = cls.build_get_cache_hash(
            pk=pk, get_type='default')
        cache_data = getattr(g, hash_str, None)
        if cache_data is not None:
            logger.info("default_query_get data retrieved from g object")
            return cache_data

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
        tmp_base_query_2 = tmp_base_query\
            .filter_by(**converted_pk)
        model_object = tmp_base_query_2.first()

        if model_object is None and raise_error:
            message = "Requested object {model_class}[{pk}] not found."
            raise PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": cls.__name__,
                    "pk": _try_convert_int(pk)})

        # Set a local cache for object using g object
        setattr(g, hash_str, model_object)
        return model_object

    @classmethod
    def query_list(cls, filter_dict: None | dict = None,
                   exclude_dict: dict = None,
                   order_by: list = None, limit: int = None,
                   base_query: Query = None) -> Query:
        """Create a list query using parameter and without default filters.

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
        tmp_base_query = cls.query if base_query is None else base_query

        query_result = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=cls,
                base_query=tmp_base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict,
                order_by=order_by)
        if limit is None:
            return query_result
        else:
            return query_result.limit(limit)

    @classmethod
    def query_get(cls, pk: str | int, base_query: Query = None,
                  raise_error: bool = True) -> object:
        """Get model_class object using pumpwood pk without base query filter.

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
        hash_str = cls.build_get_cache_hash(
            pk=pk, get_type='query')
        cache_data = getattr(g, hash_str, None)
        if cache_data is not None:
            logger.info("default_query_get data retrieved from g object")
            return cache_data

        converted_pk = CompositePkBase64Converter.load(pk)
        if isinstance(converted_pk, (int, float)):
            # If a numeric data is passed as pk it is associated with
            # 'id' field, it is necessary to convert to a dict to unpack
            # on filter_by
            converted_pk = {'id': converted_pk}

        # Use base query to filter object acording to user's permission
        tmp_base_query = cls.query if base_query is None else base_query

        # Since base query inject a filter retricting user information
        # it is not possible to use .get
        model_object = tmp_base_query\
            .filter_by(**converted_pk).first()

        if model_object is None and raise_error:
            message = "Requested object {model_class}[{pk}] not found."
            raise PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": cls.__name__,
                    "pk": _try_convert_int(pk)})

        # Set a local cache for object using g object
        setattr(g, hash_str, model_object)
        return model_object
