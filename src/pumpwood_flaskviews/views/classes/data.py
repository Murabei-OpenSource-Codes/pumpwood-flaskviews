"""Define pumpwood data views."""
import pandas as pd
import simplejson as json
from flask import request
from flask import jsonify
from sqlalchemy import inspect as alchemy_inspect
from pumpwood_communication import exceptions
from pumpwood_flaskviews.query import SqlalchemyQueryMisc
from pumpwood_flaskviews.inspection import model_has_column

# Import simple views
from .simple import PumpWoodFlaskView


class PumpWoodDataFlaskView(PumpWoodFlaskView):
    """Class view for models that hold data."""

    _view_type = "data"

    model_variables = []
    expected_cols_bulk_save = []

    def dispatch_request(self, end_point, first_arg=None, second_arg=None):
        """dispatch_request for view, add pivot end point."""
        data = None
        if request.method.lower() in ('post', 'put'):
            if request.mimetype == 'application/json':
                data = request.get_json()
            else:
                data = request.form.to_dict()
                for k in data.keys():
                    data[k] = json.loads(data[k])

        if end_point == 'pivot' and request.method.lower() == 'post':
            endpoint_dict = data or {}
            return jsonify(self.pivot(**endpoint_dict))

        if end_point == 'bulk-save' and request.method.lower() == 'post':
            endpoint_dict = data or []
            return jsonify(self.bulk_save(data_to_save=data))

        return super(PumpWoodDataFlaskView, self).dispatch_request(
            end_point, first_arg, second_arg)

    def pivot(self, filter_dict: None | dict = None,
              exclude_dict: None | dict = None, order_by: None | list = None,
              columns: None | list = None, format: str = 'list',
              variables: list = None, show_deleted: bool = False,
              add_pk_column: bool = False, limit: int = None,
              **kwargs):
        """Pivot end-point.

        Args:
            filter_dict (dict):
                Dictionary with the arguments to be used in filter.
            exclude_dict (dict):
                Dictionary with the arguments to be used in exclude.
            order_by (list):
                List of fields to be used in ordering.
            columns (list):
                Columns to be used in pivoting
            format (str):
                Format to be used in pivot, same argument used in
                pandas to_dict.
            variables (list):
                List of the columns to be returned.
            show_deleted (bool):
                If column deleted is available
                show deleted rows. By default those columns are removed.
            add_pk_column (bool):
                Add pk column to the results facilitating
                the pagination of long dataframes.
            limit (int):
                Limit results to limit n rows.
            **kwargs:
                For compatibylity of previous versions and super function.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = self.arg_or_default_order_by(order_by=order_by)
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

    def bulk_save(self, data_to_save: list):
        """Bulk save data.

        Args:
            data_to_save(list): List of dictionaries which must have
                                self.expected_cols_bulk_save.

        Return:
            Dictionary with ['saved_count'] for total of saved objects.
        """
        session = self.get_session()

        if len(self.expected_cols_bulk_save) == 0:
            raise exceptions.PumpWoodException('Bulk save not avaiable.')

        session = self.model_class.query.session
        pd_data_to_save = pd.DataFrame(data_to_save)
        pd_data_cols = set(list(pd_data_to_save.columns))

        objects_to_load = []
        if len(set(self.expected_cols_bulk_save) - pd_data_cols) == 0:
            for d in pd_data_to_save.to_dict("records"):
                new_obj = self.model_class(**d)
                objects_to_load.append(new_obj)

            try:
                session.bulk_save_objects(objects_to_load)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

            return {'saved_count': len(objects_to_load)}
        else:
            template = 'Expected columns and data columns do not match:' + \
                '\nExpected columns: {expected}' + \
                '\nData columns: {data_cols}'
            raise exceptions.PumpWoodException(
                message=template, payload={
                    "expected": list(set(self.expected_cols_bulk_save)),
                    "data_cols": list(pd_data_cols),
                })
