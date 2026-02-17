"""Module to create SQLAlchemy order by statements."""
from sqlalchemy import func
from sqlalchemy import desc
from sqlalchemy import inspect
from pumpwood_communication.exceptions import (
    PumpWoodQueryException, PumpWoodNotImplementedError)


class SqlalchemyOrderBy:
    """Class to help building order by clauses."""

    _underscore_operators = {
        'insensitive': (
            lambda c: func.lower(c)
        ),
        'iunaccent': (
            lambda c: func.lower(func.unaccent(c)),
        ),
        'unaccent': (
            lambda c: func.unaccent(c),
        ),
        'exact': (
            lambda c: c
        ),
    }

    @classmethod
    def _get_model_columns(cls, model) -> dict:
        """Inspect model and get its columns as a dictonary."""
        mapper = inspect(model)
        return dict([
            (col.key, col) for col in list(mapper.c)])

    @classmethod
    def _get_model_name(cls, model) -> str:
        """Return model name."""
        return model.__name__

    @classmethod
    def _build_arguments(cls, model, order_by: list[str]) -> list[dict]:
        """Convert order_by to a dictionary with columns and operators.

        Returns:
            Return a list with columns and a list of operators
            that must be applied over column
        """
        model_class_name = cls._get_model_name(model=model)

        # Loop over order_by statements, separaing them on colum, json_key
        # operator and ordering
        list_order_by_dict = []
        for o in order_by:
            column = None
            json_key = None
            operator = None
            ordering = None

            # Split operator and columns using __
            operator_list = o.split("__")

            # Check if there is a - at the begining of the column name
            # it will indicate a descending order
            temp_col = operator_list[0]
            if temp_col[0] == '-':
                column = temp_col[1:]
                ordering = 'desc'
            else:
                column = temp_col
                ordering = 'asc'

            # Split column name using pipe operator that indicates
            # json key
            json_list = column.split("->")
            if len(json_list) == 2:
                column = json_list[0]
                json_key = json_list[1]

            # Nested JSON is not implemented
            elif 2 < len(operator_list):
                msg = (
                    "Nested JSON operation is not implemented. "
                    "JSON operators [{json_list}], order by "
                    "'{order_by}'; Model class '{model_class_name}'")
                raise PumpWoodNotImplementedError(
                    message=msg, payload={
                        "json_list": json_list,
                        "order_by": order_by,
                        "model_class_name": model_class_name})

            if len(operator_list) == 1:
                operator = 'exact'
            elif len(operator_list) == 2:
                operator = operator_list[1]
            else:
                msg = (
                    "Order by using joins or multiple operators not "
                    "implement. Order by arguments {operator_list}; order by "
                    "'{order_by}'; Model class '{model_class_name}'")
                raise PumpWoodNotImplementedError(
                    message=msg, payload={
                        "operator_list": operator_list,
                        "order_by": order_by,
                        "model_class_name": model_class_name})

            # Table id can be treated as pk column also, convert to id
            # if column is 'pk'
            column = 'id' if column == 'pk' else column

            # List of order by dictonary
            list_order_by_dict.append({
                'column': column, 'operator': operator,
                'ordering': ordering, 'json_key': json_key})
        return list_order_by_dict

    @classmethod
    def _build_order_by_clauses(cls, model,
                                order_args: list[dict]) -> list[dict]:
        """Create a list of SQLAlchemy statements to be used on order by.

        Returns:
            Return a list of SQLAlchemy that can be passed to order by
            function on SQLAlchemy.
        """
        model_class_name = cls._get_model_name(model=model)
        model_columns = cls._get_model_columns(model=model)

        list_statements = []
        for o in order_args:
            # Retrieve alchemy column
            alchemy_column = model_columns.get(o['column'])
            if alchemy_column is None:
                msg = (
                    "Column [{column}] used on order by not present "
                    "on model [{model_class}] fields.")
                raise PumpWoodQueryException(
                    message=msg, payload={
                        'column': o['column'],
                        'model_class': model_class_name,
                        'order_args': o})

            # If using a key in a JSON field, set it as column
            if o['json_key'] is not None:
                alchemy_column = alchemy_column[o['json_key']].astext

            # Retrieve alchemy operator
            alchemy_operator = cls._underscore_operators.get(o['operator'])
            if alchemy_operator is None:
                msg = (
                    "Operator [{operator}] not implemented. "
                    "Model class [{model_class}].")
                raise PumpWoodQueryException(
                    message=msg, payload={
                        'operator': o['operator'],
                        'model_class': model_class_name,
                        'order_args': o})

            if o['ordering'] == 'asc':
                list_statements.append(
                    alchemy_operator(alchemy_column))
            elif o['ordering'] == 'desc':
                list_statements.append(
                    desc(alchemy_operator(alchemy_column)))
            else:
                msg = (
                    "Ordering not implement, call the technical people. "
                    "Error a 'SqlalchemyOrderBy'")
                raise PumpWoodNotImplementedError(msg)

        return list_statements

    @classmethod
    def build(cls, model, order_by: list[str]):
        """Build query component used on order by clause.

        Args:
            model:
                Model at which the order by will be applied.
            order_by (list[str]):
                List of the order by arguments.

        Returns:
            Returns a list of order by clauses to be used on the query.

        Raises:
            PumpWoodNotImplementedError:
                If order_by argument point to join, this is not implemented
                order only using fields of the model.
        """
        # If order by is None, does not return an empty list
        if order_by is None or len(order_by) == 0:
            return []

        order_args = cls._build_arguments(model=model, order_by=order_by)
        return cls._build_order_by_clauses(model=model, order_args=order_args)
