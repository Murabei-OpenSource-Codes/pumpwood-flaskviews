"""Module to filter row permission."""
from flask_sqlalchemy.query import Query
from pumpwood_flaskviews.inspection import model_has_column


class BaseQuery:
    """Class to query builder."""

    @classmethod
    def row_permission_filter(cls, model: Query, query: Query = None,
                              row_permission_col: str = 'row_permission_id'
                              ) -> Query:
        """Create base query filtering row perission.

        Args:
            model:
                A declarative model of sqlalchemy.
            query (Query):
                Initial query.
            row_permission_col (str):
                Column associated with row_permission. Users row permission
                will be fetched from auth app and injected as in clause at
                query.

        Returns:
            Returns a query using user's associated row permission as filter.
        """
        model_has_row_permission = model_has_column(
            model=model, column=row_permission_col)
        if query is None:
            query = model.query
        if model_has_row_permission:
            temp_col = getattr(model, row_permission_col)
            resp_query = query.filter(temp_col.in_([2]))
            return resp_query
        else:
            return query
