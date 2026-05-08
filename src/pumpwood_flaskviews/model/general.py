"""Functions and classes for flask/SQLAlchemy models."""
from typing import Literal
from dataclasses import dataclass
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, BigInteger
from flask_sqlalchemy.query import Query
from pumpwood_flaskviews.query import (
    BaseQueryABC, BaseQueryNoFilter, SqlalchemyQueryMisc)
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.exceptions import (
    PumpWoodObjectDoesNotExist, PumpWoodOtherException)
from pumpwood_communication.cache import default_cache
from pumpwood_communication.type import PumpwoodDataclassMixin
from pumpwood_flaskviews.cache import PumpwoodFlaskGCache


def _try_convert_int(value: str):
    """Helper function to set type of the pk at error payload.

    Args:
        value (str):
            The value to be converted to integer.

    Returns:
        int | str:
            The integer value if conversion is successful,
            otherwise the original string.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


@dataclass
class FlaskPumpWoodBaseModelCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for FlaskPumpWoodBaseModel.

    This dataclass is used to generate a unique hash for caching
    purposes, ensuring that the same object is not fetched multiple
    times during a single request.
    """

    authorization_token: str
    """Request authorization token."""
    model_class: str
    """Model class for the autofill field."""
    object_pk: str | int | dict
    """Pk associated with object to get the autofill field data."""
    get_type: Literal['default', 'query']
    """Identifier for the type of query (default or unfiltered)."""
    context: str = 'pumpwood-flaskviews-model-get-cache'
    """Context identifier for the cache entry."""


class FlaskPumpWoodBaseModel(DeclarativeBase):
    """Flask Sqlalchemy Database Connection.

    It adds an 'id' column automatically for all models that inherit
    from this base class.
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

    table_partition: list[str] = []
    """Specify table partitions that are applied the database. It is expected
       that tables with more than one partition at least the first one must
       be specified on the queries."""

    HASH_DICT = {
        'context': 'flaskviews--model-query-retrieve',
        'model_class': None, 'pk': None, 'get-type': None
    }
    """Template for hash dictionary."""

    @classmethod
    def build_get_cache_hash(cls, pk: str | int,
                             get_type: Literal['default', 'query']
                             ) -> str:
        """Build get hash dict.

        Args:
            pk (str | int):
                Primary key to fetch the data.
            get_type (Literal['default', 'query']):
                Type of the request that is being made. 'default' is
                cache associated with user row_permission filter and
                'query' are for caches without the row_permission filter.

        Returns:
            str:
                A string representing the hash for the cache.
        """
        hash_dict = AuthFactory.get_auth_header()
        hash_dict = {
            'context': 'flaskviews--model-query-retrieve',
            'model_class': cls.__name__, 'pk': pk,
            'get-type': get_type}
        return default_cache._generate_hash(hash_dict)

    @classmethod
    def default_filter_query(cls, query: Query = None,
                             filter_dict: dict = None,
                             exclude_dict: dict = None,
                             order_by: list[str] = None) -> Query:
        """Create a query with default filter.

        Use base query object to add default filter to objects. Base
        query object might use auth_header.

        Args:
            query (Query):
                Query used as starting point.
            filter_dict (dict):
                Dictionary to be used in filter operations. See
                pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations. See
                pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            order_by (list):
                List of fields to be used in order by operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.

        Returns:
            Query:
                SQLAlchemy Query object with default filters applied.
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
        """Create a list query using parameter and applying default filters.

        Args:
            filter_dict (dict):
                Dictionary to be used in filter operations. See
                pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations. See
                pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            order_by (list):
                List of fields to be used in order by operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            limit (int):
                Number of objects to be returned.
            base_query (Query):
                A base query to be used as initial filter.

        Returns:
            Query:
                SQLAlchemy Query object with the results.
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
    def default_query_get(cls, pk: str | int | dict,
                          base_query: Query = None,
                          raise_error: bool = True,
                          use_cache: bool = True) -> object:
        """Get model_class object using pumpwood pk.

        Pumpwood pk may be integers and base64 strings coding a
        dictionary with composite primary keys. This function
        abstracts SQLAlchemy query.get to treat both possibilities.

        It also manages a local cache to avoid multiple database calls
        during the same request.

        Args:
            pk (str | int | dict):
                Pumpwood primary key. If the pk is already a
                dictionary it will be considered ready to be passed
                to the query.
            base_query (Query):
                A base query to be used as initial filter.
            raise_error (bool):
                Raise error if object was not found.
            use_cache (bool):
                If local cache may be used to retrieve data. If base
                query is not None, cache can not be used.

        Returns:
            object:
                A SQLAlchemy object with corresponding primary key.

        Raises:
            PumpWoodObjectDoesNotExist:
                If object is not found and raise_error is True.
            PumpWoodOtherException:
                If query returns more than one object.
        """
        # Is is not possible to unify the implementation because the cache
        # for default query uses the base query filter and the query do not.
        # Unify leads to cache inconstency.
        hash_dict = FlaskPumpWoodBaseModelCacheHash(
            authorization_token=AuthFactory.get_auth_header()['Authorization'],
            model_class=cls.__name__, object_pk=pk,
            get_type='default')
        if use_cache:
            cache_data = PumpwoodFlaskGCache.get(hash_dict=hash_dict)
            if cache_data is not None:
                if isinstance(cache_data, Exception):
                    if raise_error:
                        raise cache_data
                    return None
                return cache_data

        converted_pk = None
        if isinstance(pk, dict):
            converted_pk = pk
        else:
            converted_pk = CompositePkBase64Converter.load(pk)
            if isinstance(converted_pk, (int, float)):
                # If a numeric data is passed as pk it is associated with
                # 'id' field, it is necessary to convert to a dict to unpack
                # on filter_by
                converted_pk = {'id': converted_pk}

        # Use base query to filter object acording to user's permission,
        # it is necessary to use filter_by on request because it is
        # applied over a previous id
        model_object_results = cls.default_filter_query(query=base_query)\
            .filter_by(**converted_pk).all()
        if len(model_object_results) == 0:
            # If raise_error=True, it will raise PumpWoodObjectDoesNotExist
            # indicating that the primary key was not found on database,
            # raise_error=False will return a None object, this is usefull
            # for upsert operations.
            message = "Requested object {model_class}[{pk}] not found."
            error = PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": cls.__name__,
                    "pk": _try_convert_int(pk)})
            if use_cache:
                # Set the cache on G object to avoid calling the database
                # many times for not found objects
                PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=error)
            if raise_error:
                raise error
            return None

        # If more than one object is returned, it indicates that the
        # fields used to retrive the information can not be considered
        # unique on database
        elif len(model_object_results) != 1:
            msg = (
                "Get query for {model_class}[{pk}] returned more than "
                "one object, check implementation.")
            raise PumpWoodOtherException(
                message=msg, payload={
                    "model_class": cls.__name__,
                    "pk": _try_convert_int(pk)})

        # Get first element
        model_object = model_object_results[0]

        # Is cache is not to be used, probably it is a bulk operation
        # or an update on save, setting cache may renew old data
        # on cache.
        if use_cache:
            PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=model_object)
        return model_object

    @classmethod
    def query_list(cls, filter_dict: None | dict = None,
                   exclude_dict: dict = None,
                   order_by: list = None, limit: int = None,
                   base_query: Query = None) -> Query:
        """Create a list query using parameter and without default filters.

        Args:
            filter_dict (dict):
                Dictionary to be used in filter operations. See
                pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations. See
                pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            order_by (list):
                List of fields to be used in order by operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc
                documentation.
            limit (int):
                Number of objects to be returned.
            base_query (Query):
                A base query to be used as initial filter.

        Returns:
            Query:
                SQLAlchemy Query object with the results.
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
    def query_get(cls, pk: str | int | dict, base_query: Query = None,
                  raise_error: bool = True, use_cache: bool = True) -> object:
        """Get model_class object using pumpwood pk without base query filter.

        Pumpwood pk may be integers and base64 strings coding a
        dictionary with composite primary keys. This function
        abstracts SQLAlchemy query.get to treat both possibilities.

        Args:
            pk (str | int | dict):
                Pumpwood primary key. If the pk is already a
                dictionary it will be considered ready to be passed
                to the query.
            base_query (Query):
                A base query to be used as initial filter.
            raise_error (bool):
                Raise error if object was not found.
            use_cache (bool):
                If cache can be used to retrieve the information
                locally.

        Returns:
            object:
                A SQLAlchemy object with corresponding primary key.

        Raises:
            PumpWoodObjectDoesNotExist:
                If object is not found and raise_error is True.
            PumpWoodOtherException:
                If query returns more than one object.
        """
        # Is is not possible to unify the implementation because the cache
        # for default query uses the base query filter and the query do not.
        # Unify leads to cache inconstency.
        hash_dict = FlaskPumpWoodBaseModelCacheHash(
            authorization_token=AuthFactory.get_auth_header()['Authorization'],
            model_class=cls.__name__, object_pk=pk,
            get_type='query')
        if use_cache:
            cache_data = PumpwoodFlaskGCache.get(hash_dict=hash_dict)
            if cache_data is not None:
                if isinstance(cache_data, Exception):
                    if raise_error:
                        raise cache_data
                    return None
                return cache_data

        converted_pk = None
        if isinstance(pk, dict):
            converted_pk = pk
        else:
            converted_pk = CompositePkBase64Converter.load(pk)
            if isinstance(converted_pk, (int, float)):
                # If a numeric data is passed as pk it is associated with
                # 'id' field, it is necessary to convert to a dict to unpack
                # on filter_by
                converted_pk = {'id': converted_pk}

        # Use base query if passed as parameter
        tmp_base_query = cls.query if base_query is None else base_query

        # Filter the objects acording to primary argument and treat the
        # cases when the object is not found or when the query returns more
        # then one result
        model_object_results = tmp_base_query\
            .filter_by(**converted_pk).all()
        if len(model_object_results) == 0:
            # If raise_error=True, it will raise PumpWoodObjectDoesNotExist
            # indicating that the primary key was not found on database,
            # raise_error=False will return a None object, this is usefull
            # for upsert operations.
            message = "Requested object {model_class}[{pk}] not found."
            error = PumpWoodObjectDoesNotExist(
                message=message, payload={
                   "model_class": cls.__name__,
                   "pk": _try_convert_int(pk)})
            if use_cache:
                # Set the cache on G object to avoid calling the database
                # many times for not found objects
                PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=error)
            if raise_error:
                raise error
            return None

        # If more than one object is returned, it indicates that the
        # fields used to retrive the information can not be considered
        # unique on database
        elif len(model_object_results) != 1:
            msg = (
                "Get query for {model_class}[{pk}] returned more than "
                "one object, check implementation.")
            raise PumpWoodOtherException(
                message=msg, payload={
                    "model_class": cls.__name__,
                    "pk": _try_convert_int(pk)})

        # Get first element
        model_object = model_object_results[0]

        # Is cache is not to be used, probably it is a bulk operation
        # or an update on save, setting cache may renew old data
        # on cache.
        if use_cache:
            PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=model_object)
        return model_object