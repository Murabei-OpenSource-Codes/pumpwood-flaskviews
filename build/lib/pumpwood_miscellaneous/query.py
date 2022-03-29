"""Build sqlalchemy queries from filter_dict, exclude_dict and order_by."""
from sqlalchemy.sql import operators
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import desc
from pumpwood_communication.exceptions import PumpWoodQueryException


class SqlalchemyQueryMisc():
    """Class to help buildinng queries with dictionary of list."""

    _underscore_operators = {
        'gt': lambda c, x: operators.gt(c, x),
        'lt': lambda c, x: operators.lt(c, x),
        'gte': lambda c, x: operators.ge(c, x),
        'lte': lambda c, x: operators.le(c, x),
        'in': lambda c, x: operators.in_op(c, x),

        'contains': lambda c, x: operators.contains_op(c, x),
        'icontains': lambda c, x: c.ilike('%' + x.replace('%', '%%') + '%'),
        "unaccent_icontains":
            lambda c, x: operators.contains_op(
                func.unaccent(func.lower(c)),
                func.unaccent(x.lower())),

        'exact': lambda c, x: operators.eq(c, x),
        'iexact': lambda c, x: operators.ilike_op(c, x),
        "unaccent_iexact":
            lambda c, x: operators.operators.eq(
                func.unaccent(c),
                x.lower()),

        'startswith': lambda c, x: operators.startswith_op(c, x),
        'istartswith': lambda c, x: c.ilike(x.replace('%', '%%') + '%'),
        'unaccent_istartswith':
            lambda c, x: operators.startswith_op(
                func.unaccent(func.lower(c)),
                func.unaccent(x.replace('%', '%%') + '%')),

        'endswith': lambda c, x: operators.endswith_op(c, x),
        'iendswith': lambda c, x: c.ilike('%' + x.replace('%', '%%')),
        'unaccent_iendswith':
            lambda c, x: operators.endswith_op(
                func.unaccent(func.lower(c)),
                func.unaccent(x.lower())),

        'isnull': lambda c, x: x and c is not None or c is None,
        'range': lambda c, x: operators.between_op(c, x),
        'year': lambda c, x: func.extract('year', c) == x,
        'month': lambda c, x: func.extract('month', c) == x,
        'day': lambda c, x: func.extract('day', c) == x,
        "json_contained_by": lambda c, x: c.contained_by(x),
        "json_containshas_all": lambda c, x: c.containshas_all(x),
        "json_has_any": lambda c, x: c.has_any(x),
        "json_has_key": lambda c, x: c.has_key(x),
    }

    @classmethod
    def get_related_models_and_columns(cls, object_model, query_dict,
                                       order=False):
        """
        Get related model and columns.

        Receive a Django like dictionary and return a dictionary with the
        related models mapped by query string and the columns and operators to
        be used.
        Can also be used for order_by arguments where the keys of the query
        dict especify the joins and the value must be either asc or desc for
        ascendent and decrescent ordenation respectively.

        Args:
            object_model (sqlalchemy.DeclarativeModel): Model over which will
                be performed the queries.
            query_dict (dict): A query dict similar to Django queries,
                with relations and operator divided
            by "__".

        Kwargs:
            No extra arguments

        Returns:
            dict: Key 'models' indicates models to be used in joins and
            'columns' returns a list o dictionaries with 'column' for model
            column, 'operation' for operation to be used and 'value' for
            value in operation.

        Raises:
            PumpWoodQueryException (It is not permited more tokens after
                                    operation underscore (%s).)
                Original query string (%s)) If a operation_key is recognized
                and there is other relations after it.
                Ex.: attribute__in__database_set

            PumpWoodQueryException(It is not possible to continue building
                                   query, underscore token ({token}) not found
                                   on model columns, relations or operations.
                                   Original query string:...)
                If a token (value separated by "__" is not reconized as neither
                relations, column and operation)
                Ex: attribute__description__last_update_at

            PumpWoodQueryException('Order value %s not implemented , sup and
                                   desc avaiable, for column %s. Original query
                                   string %s')
                If value in query dict when order=True is different from
                'asc' and 'desc'.

        Example:
            >>> filter_query = get_related_models_and_columns(
                object_model=DataBaseVariable,
                query_dict={
                    'attribute__description__contains': 'Chubaca' ,
                    'value__gt': 2})
            >>> q = object_model.query.join(*filter_query['models'])
            >>> for fil in filter_query['columns']:
            >>>     q = q.filter(
                fil['operation'](fil['column'], fil['value']))

        """
        join_models = []
        columns_values_filter = []
        for arg, value in query_dict.items():
            operation_key = None
            column = None
            json_key = None
            actual_model = object_model
            # q = object_model.query.with_entities(object_model.id)
            # token = arg.split('__')[0]
            for token in arg.split('__'):
                # Check if it is to check a json key
                json_list = token.split("->")
                if len(json_list) != 1:
                    json_key = json_list[1]
                    token = json_list[0]

                # operation_key must be the last token
                if operation_key is not None:
                    template = "It is not permited more tokens after " + \
                        "operation underscore (%s). Original query string (%s)"
                    raise PumpWoodQueryException(
                        template % (operation_key, arg))

                mapper = inspect(actual_model)
                relations = dict([
                    (r.key, [r.mapper.class_, r.primaryjoin])
                    for r in list(mapper.relationships)])
                columns = dict([
                    (col.key, col) for col in list(mapper.c)])

                # Check if a search for a relation
                if token in relations.keys():
                    # It is not possible to query for relations after specifing
                    # a collumn
                    if column is not None:
                        template = "It is not permited more relations " + \
                            "after column underscore (%s). Original query " + \
                            "string (%s)"
                        raise PumpWoodQueryException(
                            template % (column.key, arg))

                    actual_model = relations[token][0]
                    join_models.append(relations[token])

                # Check if is search for primary_key
                elif token == 'pk':
                    column = mapper.primary_key[0]

                # Check if is search for column
                elif token in columns.keys():
                    if column is not None:
                        template = "It is not permited more columns after " +\
                            "column underscore (%s). Original query " + \
                            "string (%s)"
                        raise PumpWoodQueryException(
                            template % (column.key, arg))
                    column = columns[token]
                elif token in cls._underscore_operators.keys():
                    operation_key = token
                else:
                    msg = 'It is not possible to continue building query, ' + \
                        'underscore token ({token}) not found on model ' + \
                        'columns, relations or operations. Original query ' + \
                        'string: "{query}".\n'
                    msg = msg + 'Columns: {cols}\n'
                    msg = msg + 'Relations: {rels}\n'
                    msg = msg + 'Operations: {opers}%s'
                    final_msg = msg.format(
                        token=token, query=arg,
                        cols=str(list(columns.keys())),
                        rels=str(list(relations.keys())),
                        opers=str(list(cls._underscore_operators.keys())))
                    raise PumpWoodQueryException(final_msg)

            if order:
                if value not in ['asc', 'desc']:
                    template = "Order value %s not implemented , sup and " + \
                        "desc avaiable, for column %s. Original query " + \
                        "string %s"
                    raise PumpWoodQueryException(
                        template % (value, column.key, arg))
                else:
                    if json_key is not None:
                        if value == 'desc':
                            columns_values_filter.append(
                                {'column': column[json_key].astext,
                                 'operation': desc})
                        elif value == 'asc':
                            columns_values_filter.append(
                                {'column': column[json_key].astext,
                                 'operation': lambda c: c})
                    else:
                        if value == 'desc':
                            columns_values_filter.append(
                                {'column': column, 'operation': desc})
                        elif value == 'asc':
                            columns_values_filter.append(
                                {'column': column, 'operation': lambda c: c})
            else:
                if operation_key is None:
                    operation_key = 'exact'
                if json_key is not None:
                    columns_values_filter.append(
                        {'column': column[json_key].astext,
                         'operation': cls._underscore_operators[operation_key],
                         'value': value})
                else:
                    columns_values_filter.append(
                        {'column': column,
                         'operation': cls._underscore_operators[operation_key],
                         'value': value})

        return {'models': join_models, 'columns': columns_values_filter}

    @classmethod
    def sqlalchemy_kward_query(cls, object_model, filter_dict={},
                               exclude_dict={}, order_by=[]):
        """
        Build SQLAlchemy engine string acordind to database parameters.

        Args:
            filter_dict (dict): Dictionary to be used in filtering.
            exclude_dict (dict): Dictionary to be used in excluding.
            order_by (list): Dictionary to be used as ordering.
        Kwargs:
            No extra arguments

        Raises:
            No raises implemented

        Return:
            sqlalquemy.query: Returns an sqlalchemy with filters applied.

        Example:
        >>> query = SqlalchemyQueryMisc.sqlalchemy_kward_query(
                object_model=DataBaseVariable
                filter_dict={'attribute__description__contains': 'Oi' ,
                             'value__gt': 2}
                exclude_dict={'modeling_unit__description__exact': 'Mod_3'}
                order_by = ['-value', 'attribute__description'])

        """
        order_by_dict = {}
        for o in order_by:
            if o[0] == '-':
                order_by_dict[o[1:]] = 'desc'
            else:
                order_by_dict[o] = 'asc'

        filter_query = cls.get_related_models_and_columns(
            object_model=object_model, query_dict=filter_dict)
        exclude_query = cls.get_related_models_and_columns(
            object_model=object_model, query_dict=exclude_dict)
        order_query = cls.get_related_models_and_columns(
            object_model, order_by_dict, order=True)

        models = list(
            filter_query['models'] + exclude_query['models'] +
            order_query['models'])

        # Join models for filters
        q = object_model.query
        for join_models in models:
            q = q.join(join_models[0], join_models[1])

        # Filter clauses
        for fil in filter_query['columns']:
            q = q.filter(fil['operation'](fil['column'], fil['value']))
        # Exclude clauses
        for excl in exclude_query['columns']:
            q = q.filter(~excl['operation'](c=excl['column'], x=excl['value']))
        # Order clauses
        for ord in order_query['columns']:
            q = q.order_by(ord['operation'](ord['column']))
        return q
