"""Module to filter row permission."""
import abc
import json
from flask import request
from flask_sqlalchemy.query import Query
from sqlalchemy.orm import DeclarativeBase
from pumpwood_flaskviews.inspection import model_has_column
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_communication.exceptions import (
    PumpWoodOtherException, PumpWoodQueryException)


class BaseQueryABC(abc.ABC):
    """Abstract base class for base query class."""

    def add_filter(self, model: DeclarativeBase, query: Query = None,
                   filter_dict: None | dict = None, exclude_dict: dict = None,
                   order_by: list = None) -> Query:
        """It is necessary to implement base_query.

        Args:
            model (DeclarativeBase):
                Model that will return the objects associated with the
                base query.
            query (Query):
                Previous query statment, it can be used to concatenate
                more than one base query.
            filter_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by (list):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.

        Returns:
            Returns a base query to be used on model fetch.
        """
        pass

    @staticmethod
    def validate_skip_arg(base_filter_skip: list[str]) -> list[str]:
        """Validate base_filter_skip retrieved from URL parameters.

        Args:
            base_filter_skip (list[str]):
                JSON loaded base_filter_skip parameter.

        Returns:
            The same data from base_filter_skip.

        Raises:
            PumpWoodQueryException:
                If base_filter_skip it is not a list of strings.
        """
        error_msg = (
            "base_filter_skip URL parameter must be a list of strings, "
            "it is expected to be a serialized json list, "
            "check implementation.\nbase_filter_skip value: "
            "{base_filter_skip}")
        if not isinstance(base_filter_skip, list):
            raise PumpWoodQueryException(
                message=error_msg,
                payload={"base_filter_skip": base_filter_skip})
        for x in base_filter_skip:
            if not isinstance(x, str):
                raise PumpWoodQueryException(
                    message=error_msg,
                    payload={"base_filter_skip": base_filter_skip})
        return base_filter_skip

    def get_skip_arg(self) -> list[str]:
        """Retrieve skip base filter from URL."""
        # Try to load URL parameter as a JSON
        request_base_filter_skip = request.args.get('base_filter_skip', '[]')
        try:
            base_filter_skip = json.loads(request_base_filter_skip)
        except json.JSONDecodeError:
            error_msg = (
                "base_filter_skip URL parameter must be a list of strings, "
                "it is expected to be a serialized JSON list. It was not "
                "possible to load base_filter_skip as a JSON, check object "
                "serialization. \nRequest base_filter_skip parameter value: "
                "{request_base_filter_skip}")
            raise PumpWoodQueryException(
                message=error_msg,
                payload={"request_base_filter_skip": request_base_filter_skip})

        if base_filter_skip is None:
            base_filter_skip = []
        return self.validate_skip_arg(base_filter_skip=base_filter_skip)

    def check_for_skip_arg(self, skip_arg: str = None) -> bool:
        """Check if skip arg is preesent on request URL.

        It will also look for 'ALL', it is expected that 'ALL' skip arg will
        skip all filter for different base query.

        Args:
            skip_arg (str):
                Skip argument that will be checked if present on
                base_filter_skip URL parameter.

        Returns:
            Return True if skip_arg if present on URL parameter or if
            'ALL' is present.
        """
        if skip_arg is None:
            return False
        else:
            request_skip_arg = self.get_skip_arg()
            return (
                (skip_arg in request_skip_arg) or
                ('ALL' in request_skip_arg)
            )


class BaseQueryNoFilter(BaseQueryABC):
    """Dummy base query that will not change the behavior."""

    def add_filter(self, model: DeclarativeBase, query: Query = None,
                   **kwards) -> Query:
        """It is necessary to implement base_query.

        Args:
            model (DeclarativeBase):
                Model that will return the objects associated with the
                base query.
            query (Query):
                Previous query statment, it can be used to concatenate
                more than one base query.
            **kwards (dict):
                Other named parameters.

        Returns:
            Returns a base query to be used on model fetch.
        """
        if query is None:
            query = model.query
        return query


class BaseQueryRowPermission(BaseQueryABC):
    """Class to query builder."""

    def __init__(self, row_permission_col: str):
        """Intanciate an object to perform row_permission filter.

        Args:
            row_permission_col (str):
                Name of the columns to be considered as row_permission_id.
        """
        self.row_permission_col = row_permission_col

    def add_filter(self, model: DeclarativeBase, query: Query = None,
                   **kwards) -> Query:
        """Create base query filtering row perission.

        Args:
            model (DeclarativeBase):
                A declarative model of sqlalchemy.
            query (Query):
                Initial query.
            **kwards (dict):
                Other named parameters.

        Returns:
            Returns a query using user's associated row permission as filter.
        """
        model_has_col = model_has_column(
            model=model, column=self.row_permission_col)
        if not model_has_col:
            msg = (
                "Base query [BaseQueryRowPermission] not correcly " +
                "configured for model [{model}], owner column " +
                "[{row_permission_col}] does not exist at the model.")\
                .format(
                    model=model.__name__,
                    row_permission_col=self.row_permission_col)
            raise PumpWoodOtherException(msg)

        if query is None:
            query = model.query

        is_skip = self.check_for_skip_arg(
            skip_arg='BaseQueryRowPermission')
        user_info = AuthFactory.retrieve_authenticated_user()
        if is_skip and user_info['is_superuser']:
            return query

        # Check row permission associated with user
        else:
            row_permission_set = [
                x['pk'] for x in user_info['all_row_permisson_set']]
            temp_col = getattr(model, self.row_permission_col)
            return query.filter(temp_col.in_(row_permission_set))


class BaseQueryOwner(BaseQueryABC):
    """Class to query builder."""

    def __init__(self, owner_col: str):
        """Intanciate an object to perform owner filter.

        It will filter rows using the owner_col match user logged.

        Args:
            owner_col (str):
                Name of the columns to be considered as owner to match
                logged user 'pk'.
        """
        self.owner_col = owner_col

    def add_filter(self, model: DeclarativeBase, query: Query = None,
                   **kwards) -> Query:
        """Create base query filtering row perission.

        Args:
            model (DeclarativeBase):
                A declarative model of sqlalchemy.
            query (Query):
                Initial query.
            **kwards (dict):
                Other named parameters.

        Returns:
            Returns a query using logged user match `owner_col`.
        """
        model_has_col = model_has_column(
            model=model, column=self.owner_col)
        if not model_has_col:
            msg = (
                "Base query [BaseQueryOwner] not correcly configured for "
                "model [{model}], owner column [{owner_col}] does not exist "
                "at the model.")\
                .format(
                    model=model.__name__,
                    owner_col=self.owner_col)
            raise PumpWoodOtherException(msg)

        if query is None:
            query = model.query

        user_info = AuthFactory.retrieve_authenticated_user()
        is_skip = self.check_for_skip_arg(skip_arg='BaseQueryOwner')
        if is_skip and user_info['is_superuser']:
            return query

        # Check if object is owned by logged user
        else:
            temp_col = getattr(model, self.owner_col)
            resp_query = query.filter(temp_col == user_info['pk'])
            return resp_query


class BaseFilterDeleted(BaseQueryABC):
    """Class to base filter for deleted objects."""

    def __init__(self, deleted_col: str):
        """Intanciate an object to perform deleted filter.

        Will filter objects singed with deleted=False only.

        Args:
            deleted_col (str):
                Name of the columns to be considered as owner to match
                logged user 'pk'.
        """
        self.deleted_col = deleted_col

    def add_filter(self, model: DeclarativeBase, query: Query = None,
                   **kwards) -> Query:
        """Create base query filtering row perission.

        Args:
            model (DeclarativeBase):
                A declarative model of sqlalchemy.
            query (Query):
                Initial query.
            **kwards (dict):
                Other named parameters.

        Returns:
            Returns a query using logged user match `owner_col`.
        """
        model_has_col = model_has_column(
            model=model, column=self.deleted_col)
        if not model_has_col:
            msg = (
                "Base query [BaseFilterDeleted] not correcly configured for "
                "model [{model}], owner column [{owner_col}] does not exist "
                "at the model.")\
                .format(
                    model=model.__name__,
                    owner_col=self.owner_col)
            raise PumpWoodOtherException(msg)

        if query is None:
            query = model.query

        user_info = AuthFactory.retrieve_authenticated_user()
        is_skip = self.check_for_skip_arg(skip_arg='BaseFilterDeleted')
        if is_skip and user_info['is_superuser']:
            return query

        # Check if object is owned by logged user
        else:
            temp_col = getattr(model, self.deleted_col)
            resp_query = query.filter(temp_col.is_(False))
            return resp_query
