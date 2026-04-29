"""Define pumpwood data views."""
import pandas as pd
import simplejson as json
import numpy as np
from typing import Union
from flask import request, jsonify, Response
from sqlalchemy import inspect as alchemy_inspect
from pumpwood_communication import exceptions
from pumpwood_flaskviews.query import SqlalchemyQueryMisc
from pumpwood_flaskviews.inspection import model_has_column
from pumpwood_flaskviews.views.classes.data.aux import FillBulkSaveFields
from pumpwood_flaskviews.views.classes.simple import PumpWoodFlaskView
from pumpwood_flaskviews.exceptions import PumpWoodFlaskViewEndPointFoundError


class PumpWoodDataFlaskView(PumpWoodFlaskView):
    """View class for models holding large datasets.

    Provides specialized endpoints for pivoting data and performing
    high-performance bulk insertions.
    """

    _view_type = "data"

    model_variables = []
    expected_cols_bulk_save = []

    def dispatch_request(self, end_point: str, first_arg: str = None,
                         second_arg: str = None) -> Response:
        """Dispatch the request, including pivot and bulk-save endpoints.

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
                If the endpoint or method combination is not implemented.
        """
        # Check if it is possible to treat request using simple,
        # if not check for dimension end-point, if not than raise not found
        # error
        try:
            return super().dispatch_request(end_point, first_arg, second_arg)

        except PumpWoodFlaskViewEndPointFoundError as e:
            # Treat request payload
            data = self._get_request_payload(request=request) or {}

            if end_point == 'pivot' and request.method.lower() == 'post':
                return jsonify(self.pivot(**data))

            if end_point == 'bulk-save' and request.method.lower() == 'post':
                return jsonify(self.bulk_save(data_to_save=data))
            raise e

    def pivot(self, filter_dict: dict = None,
              exclude_dict: dict = None, order_by: list = None,
              columns: list = None, format: str = 'list',
              variables: list = None, show_deleted: bool = False,
              add_pk_column: bool = False, limit: int = None,
              **kwargs) -> Union[dict, list]:
        """Query data in a long format and pivot it based on columns.

        Args:
            filter_dict (dict):
                Filters to apply before pivoting.
            exclude_dict (dict):
                Exclusions to apply before pivoting.
            order_by (list):
                Ordering criteria for the source query.
            columns (list):
                Fields to be used as pivot columns.
            format (str):
                Pandas dictionary format for the output.
            variables (list):
                Fields to include in the query (unpivoted).
            show_deleted (bool):
                If True, includes soft-deleted rows.
            add_pk_column (bool):
                If True, adds primary keys to ensure row uniqueness.
            limit (int):
                Maximum number of source rows to process.
            **kwargs:
                For compatibility and extensibility.

        Returns:
            Union[dict, list]:
                The pivoted data in the requested format.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by
        columns = [] if columns is None else columns
        self.get_session()

        model_variables = variables or self.model_variables
        if type(columns) is not list:
            raise exceptions.PumpWoodException(
                'Columns must be a list of elements.')

        if len(set(columns) - set(model_variables)) != 0:
            raise exceptions.PumpWoodException(
                'Column chosen as pivot is not at model variables')

        if format not in ['dict', 'list', 'series', 'split',
                          'records', 'index']:
            raise exceptions.PumpWoodException(
                "Format must be in ['dict','list','series','split'," +
                "'records','index']")

        # Remove deleted entries from results
        if model_has_column(self.model_class, column='deleted'):
            if not show_deleted:
                filter_dict["deleted"] = False

        # Add pk/id columns to results
        if add_pk_column:
            if len(columns) != 0:
                msg = (
                    "Can not add pk column and pivot information, "
                    "information must be requested on a long format since "
                    "primary keys together are unique")
                raise exceptions.PumpWoodException(msg)

            for pk_col in self.get_primary_keys():
                if (pk_col not in model_variables):
                    model_variables = [pk_col] + model_variables

        # Use base query to limit user access
        base_query = self._add_default_filter()
        query = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=self.model_class,
                base_query=base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict,
                order_by=order_by)

        # Limit results to help on pagination
        if limit is not None:
            query = query.limit(limit)

        # Set columns to be returned at query
        variables_to_return = [
            col for col in list(alchemy_inspect(self.model_class).c)
            if col.key in model_variables]
        melted_data = pd.DataFrame(
            query.with_entities(*variables_to_return).all())

        if len(columns) == 0:
            response = melted_data.to_dict(format)
        elif melted_data.shape[0] == 0:
            if format == 'records':
                response = []
            else:
                response = {}
        else:
            if 'value' not in melted_data.columns:
                raise exceptions.PumpWoodException(
                    "'value' column not at melted data, it is not possible"
                    " to pivot dataframe.")
            index = list(set(model_variables) - set(columns + ['value']))
            pivoted_table = pd.pivot_table(
                melted_data, values='value', index=index,
                columns=columns)
            pivoted_table = pivoted_table.where(
                pd.notna(pivoted_table), None)
            response = pivoted_table.reset_index().to_dict(format)

        if type(response) is dict:
            response = {str(k): v for k, v in response.items()}
        return response

    def bulk_save(self, data_to_save: list) -> int:
        """Perform a high-performance bulk insertion of records.

        Args:
            data_to_save (list):
                A list of dictionaries containing the object data.

        Returns:
            int:
                The total number of records successfully saved.

        Raises:
            PumpWoodException:
                If bulk saving is not enabled for the view.
        """
        session = self.get_session()

        if len(self.expected_cols_bulk_save) == 0:
            raise exceptions.PumpWoodException('Bulk save not avaiable.')

        session = self.db.session
        pd_data_to_save = pd.DataFrame(data_to_save)

        # Replace NaN for None to insert on the database
        pd_data_to_save = FillBulkSaveFields.run(
            data=pd_data_to_save,
            fields=self.expected_cols_bulk_save,
            microservice=self.microservice)\
            .replace({np.nan: None})

        try:
            session.bulk_insert_mappings(
                self.model_class, pd_data_to_save.to_dict("records"))
            session.commit()
            return len(pd_data_to_save)
        except Exception as e:
            session.rollback()
            raise e
