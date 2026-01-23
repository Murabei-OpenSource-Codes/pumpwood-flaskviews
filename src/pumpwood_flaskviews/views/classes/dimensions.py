"""Define pumpwood dimension view."""
import pandas as pd
import simplejson as json
from flask import request
from flask import jsonify
from sqlalchemy.sql import text
from pumpwood_communication import exceptions
from pumpwood_flaskviews.query import SqlalchemyQueryMisc

# Classes views
from .simple import PumpWoodFlaskView


class PumpWoodDimensionsFlaskView(PumpWoodFlaskView):
    """Class view for models that hold data."""

    _view_type = "dimension"

    def dispatch_request(self, end_point, first_arg=None, second_arg=None):
        """dispatch_request for view, add pivot end point."""
        # Check if it is possible to treat request using simple,
        # if not check for dimension end-point, if not than raise not found
        # error
        try:
            return super().dispatch_request(end_point, first_arg, second_arg)
        except exceptions.PumpWoodException as e:
            # Load payload from request
            if request.method.lower() in ('post', 'put'):
                if request.mimetype == 'application/json':
                    data = request.get_json()
                else:
                    data = request.form.to_dict()
                    for k in data.keys():
                        data[k] = json.loads(data[k])

            if (end_point == 'list-dimensions' and
                    request.method.lower() == 'post'):
                endpoint_dict = data or {}
                return jsonify(self.list_dimensions(**endpoint_dict))

            if (end_point == 'list-dimension-values' and
                    request.method.lower() == 'post'):
                endpoint_dict = data or {}
                if "key" not in endpoint_dict.keys():
                    raise exceptions.PumpWoodException(
                        "Dimention key must be passed as post payload "
                        "{key: [value]}")
                return jsonify(self.list_dimension_values(**endpoint_dict))
            raise e

    def list_dimensions(self, filter_dict: dict = {},
                        exclude_dict: dict = {}) -> list:
        """List dimensions avaiable using query.

        Args:
            filter_dict : dict
                Filter query dict to get avaiable dimensions.
            exclude_dict : dict
                Exclude query dict to get avaiable dimensions.

        Returns:
            List of the avaiable keys on dimensions database.
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
        """List dimensions avaiable using query.

        Args:
            key (str):
                Key to list possible values in database.
            filter_dict (dict):
                Filter query dict to get avaiable dimensions.
            exclude_dict (dict):
                Exclude query dict to get avaiable dimensions.

        Returns:
            List of the avaiable values for key dimention.
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
