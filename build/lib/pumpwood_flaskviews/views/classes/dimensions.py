"""Define pumpwood dimension view."""
import pandas as pd
import simplejson as json
from flask import request, jsonify, Response
from sqlalchemy.sql import text
from pumpwood_communication import exceptions
from pumpwood_flaskviews.query import SqlalchemyQueryMisc

# Classes views
from .simple import PumpWoodFlaskView
from pumpwood_flaskviews.exceptions import PumpWoodFlaskViewEndPointFoundError


class PumpWoodDimensionsFlaskView(PumpWoodFlaskView):
    """View class for models containing dimension-serialized data.

    Extends basic CRUD logic with specialized endpoints for querying
    keys and values within JSONB dimension columns.
    """

    _view_type = "dimension"

    def dispatch_request(self, end_point: str, first_arg: str = None,
                         second_arg: str = None) -> Response:
        """Dispatch the request, including specialized dimension endpoints.

        Args:
            end_point (str):
                The endpoint identifier.
            first_arg (str):
                The first URL argument.
            second_arg (str):
                The second URL argument.

        Returns:
            Response:
                The Flask response object.

        Raises:
            PumpWoodException:
                If the endpoint exists but has invalid payload or method.
        """
        # Check if it is possible to treat request using simple,
        # if not check for dimension end-point, if not than raise not found
        # error
        try:
            return super().dispatch_request(end_point, first_arg, second_arg)

        except PumpWoodFlaskViewEndPointFoundError as e:
            data = self._get_request_payload(request=request) or {}

            is_list_dimensions = (
                (end_point == 'list-dimensions') and
                (request.method.lower() == 'post'))
            if (is_list_dimensions):
                return jsonify(self.list_dimensions(**data))

            is_dimension_values = (
                (end_point == 'list-dimension-values') and
                (request.method.lower() == 'post'))
            if (is_dimension_values):
                if "key" not in data.keys():
                    raise exceptions.PumpWoodException(
                        "Dimention key must be passed as post payload "
                        "{key: [value]}")
                return jsonify(self.list_dimension_values(**data))
            raise e

    def list_dimensions(self, filter_dict: dict = None,
                        exclude_dict: dict = None) -> list:
        """List distinct dimension keys available within the filtered query.

        Args:
            filter_dict (dict):
                Filters to apply before extracting keys.
            exclude_dict (dict):
                Exclusions to apply before extracting keys.

        Returns:
            list:
                A list of available keys found in dimension columns.
        """
        self.get_session()
        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict

        # Limit access to data
        base_query = self._add_default_filter()
        query_string = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=self.model_class,
                base_query=base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict)\
                .statement.compile(compile_kwargs={"literal_binds": True})\
                .string

        sql_statement = """
            SELECT DISTINCT jsonb_object_keys(dimensions) AS keys
            FROM (
                {query_string}
            ) sub
            ORDER BY keys
        """.format(query_string=query_string) # NOQA Controlled Input
        distinct_keys = pd.read_sql(
            text(sql_statement), con=self.db.engine)\
            .loc[:, "keys"]
        return distinct_keys

    def list_dimension_values(self, key: str, filter_dict: dict = {},
                              exclude_dict: dict = {}) -> list:
        """List distinct values for a specific dimension key.

        Args:
            key (str):
                The dimension key to inspect.
            filter_dict (dict):
                Filters to apply before extracting values.
            exclude_dict (dict):
                Exclusions to apply before extracting values.

        Returns:
            list:
                A list of distinct values associated with the dimension key.
        """
        self.get_session()

        # Use base query to filter user access to data according to
        # row permission
        base_query = self._add_default_filter()
        query_string = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=self.model_class,
                base_query=base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict)\
                .statement.compile(compile_kwargs={"literal_binds": True})\
                .string

        sql_statement = """
            SELECT DISTINCT dimensions -> '{key}' AS value
            FROM (
                {query_string}
            ) sub
            WHERE dimensions -> '{key}' IS NOT NULL
            ORDER BY value
        """.format(query_string=query_string, key=key) # NOQA Controlled Input
        distinct_values = pd.read_sql(text(sql_statement), con=self.db.engine)\
            .loc[:, "value"]
        return distinct_values
