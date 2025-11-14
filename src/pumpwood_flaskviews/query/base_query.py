"""Module to filter row permission."""
import abc
from flask_sqlalchemy.query import Query
from sqlalchemy.orm import DeclarativeBase
from pumpwood_flaskviews.inspection import model_has_column
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_communication.exceptions import PumpWoodOtherException


class BaseQueryABC(abc.ABC):
    """Abstract base class for base query class."""

    @abc.abstractmethod
    def base_query(self, model: DeclarativeBase, query: Query = None) -> Query:
        """It is necessary to implement base_query.

        Args:
            model (DeclarativeBase):
                Model that will return the objects associated with the
                base query.
            query (Query):
                Previus query statment, it can be used to concatenate
                more than one base query.

        Returns:
            Returns a base query to be used on model fetch.
        """
        pass


class BaseQueryRowPermission(BaseQueryABC):
    """Class to query builder."""

    def __init__(self, row_permission_col: str = 'row_permission_id'):
        """Intanciate an object to perform row_permission filter.

        Args:
            row_permission_col (str):
                Name of the columns to be considered as row_permission_id.
        """
        self.row_permission_col = row_permission_col

    def base_query(self, model: DeclarativeBase, query: Query = None) -> Query:
        """Create base query filtering row perission.

        Args:
            model (DeclarativeBase):
                A declarative model of sqlalchemy.
            query (Query):
                Initial query.

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

        user_info = AuthFactory.retrieve_authenticated_user()
        if user_info['is_superuser']:
            return query

        # Check row permission associated with user
        else:
            row_permission_set = [
                x['pk'] for x in user_info['row_permission_set']]
            temp_col = getattr(model, self.row_permission_col)
            return query.filter(temp_col.in_(row_permission_set))


class BaseQueryOwner(BaseQueryABC):
    """Class to query builder."""

    def __init__(self, owner_col: str = 'owner_id'):
        """Intanciate an object to perform owner filter.

        It will filter rows using the owner_col match user logged.

        Args:
            owner_col (str):
                Name of the columns to be considered as owner to match
                logged user 'pk'.
        """
        self.owner_col = owner_col

    def base_query(self, model: DeclarativeBase, query: Query = None) -> Query:
        """Create base query filtering row perission.

        Args:
            model (DeclarativeBase):
                A declarative model of sqlalchemy.
            query (Query):
                Initial query.

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
        # Super users has access to all row_permissions
        if user_info['is_superuser']:
            return query

        # Check if object is owned by logged user
        else:
            temp_col = getattr(model, self.owner_col)
            resp_query = query.filter(temp_col == user_info['pk'])
            return resp_query
