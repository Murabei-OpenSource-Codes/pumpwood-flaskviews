"""Build sqlalchemy queries from filter_dict, exclude_dict and order_by."""
import copy
import numpy as np
import pandas as pd
import flask_sqlalchemy
from sqlalchemy.sql import operators
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import desc
from pumpwood_communication.exceptions import (
    PumpWoodQueryException, PumpWoodNotImplementedError)
from pumpwood_communication.serializers import CompositePkBase64Converter


def open_composite_pk(query_dict: dict, is_filter: bool) -> dict:
    """Open filter/exclude dictionary with pk on composite primary keys.

    Open filter dict to filter all components of the composite primary
    keys. For exclude dict use just the id field from the composite
    primary.

    Args:
        query_dict (dict):
            Query dictionary containing information of the filters, exclude
            and order by that will be applied.
        is_filter (bool):
            If the pk will be used on filter or on exclude clauses.

    Kwargs:
        No kwargs.

    Return [dict]:
        Dictionary with adjusted filter and exclude dictionaries.
    """

    def convert_np(obj):
        """Help to treat numpy types that are not converted by SQLAlchemy."""
        if isinstance(obj, np.generic):
            return obj.item()
        else:
            return obj

    # Id is always unique even in partitioned tables,
    # but using other fields helps Postgres to find
    # information.
    #
    # Filter queries, use both id and other primary keys elements to
    # help Postgres to prune sub-partitions on query execution.
    #
    # On exclude query, using just id is the same of including all
    # composite primary fields. Since exclude filter might not lead to
    # partitions prune, they are excluded from dictionary.
    query_dict_keys = query_dict.keys()
    new_query_dict = copy.deepcopy(query_dict)
    for key in query_dict_keys:
        count_pk_filters = 0
        if "pk" in key:
            if key == "pk":
                if is_filter:
                    open_composite = CompositePkBase64Converter.load(
                        new_query_dict["pk"])
                    new_query_dict.update(open_composite)
                else:
                    open_composite = CompositePkBase64Converter.load(
                        new_query_dict["pk"])
                    new_query_dict["id"] = open_composite["id"]

                count_pk_filters = count_pk_filters + 1
                del new_query_dict["pk"]

            elif key == "pk__in":
                if is_filter:
                    open_composite = pd.DataFrame(
                            pd.Series(new_query_dict["pk__in"]).apply(
                                CompositePkBase64Converter.load).tolist())
                    for col in open_composite.columns:
                        new_query_dict[col + "__in"] = [
                            convert_np(x)
                            for x in open_composite[col].unique()]
                else:
                    open_composite = pd.DataFrame(
                            pd.Series(new_query_dict["pk__in"]).apply(
                                CompositePkBase64Converter.load).tolist())
                    new_query_dict["id__in"] = [
                        convert_np(x) for x in open_composite["id"].unique()]

                count_pk_filters = count_pk_filters + 1
                del new_query_dict["pk__in"]

            else:
                temp_msg_dict = (
                    "filter_dict" if is_filter else "exclude_dict")
                msg = (
                    "Using composite pk to filter queries must use just "
                    "equal and in operators and with join. "
                    "{temp_msg_dict}: \n {query_dict}").format(
                        temp_msg_dict=temp_msg_dict,
                        query_dict=query_dict.keys())
                raise PumpWoodQueryException(
                    message=msg, payload={
                        "query_dict": query_dict,
                        "is_filter": is_filter})

        if 1 < count_pk_filters:
            msg = (
                "Please give some help for the dev here, use just one "
                "filter_dict entry for composite primary key...")
            raise PumpWoodQueryException(
                message=msg, payload={
                    "query_dict": query_dict,
                    "is_filter": is_filter})
    return new_query_dict


class SqlalchemyQueryMisc():
    """Class to help building queries with dictionary of list."""

    _underscore_operators = {
        'eq': lambda c, x: operators.eq(c, x),
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

        # Trigram text search
        "similarity": operators.custom_op("%%"),
        "word_similar_left": operators.custom_op("<%%"),
        "word_similar_right": operators.custom_op("%%>"),
        "strict_word__similar_left": operators.custom_op("<<%%"),
        "strict_word__similar_right": operators.custom_op("%%>>"),
    }

    @classmethod
    def get_related_models_and_columns(cls, object_model, query_dict,
                                       order=False):
        """Get related model and columns.

        Receive a Django like dictionary and return a dictionary with the
        related models mapped by query string and the columns and operators to
        be used.

        Can also be used for order_by arguments where the keys of the query
        dict specify the joins and the value must be either asc or desc for
        ascendent and decrescent order respectively.

        Args:
            object_model (sqlalchemy.DeclarativeModel):
                Model over which will be performed the queries.
            query_dict (dict):
                A query dict similar to Django queries, with relations and
                operator divided by "__".
            order (bool):
                If the relation will be used on ordering operations.

        Kwargs:
            No extra arguments

        Returns:
            dict: Key 'models' indicates models to be used in joins and
            'columns' returns a list o dictionaries with 'column' for model
            column, 'operation' for operation to be used and 'value' for
            value in operation.

        Raises:
            PumpWoodQueryException (It is not permitted more tokens after
                                    operation underscore (%s).)
                Original query string (%s)) If a operation_key is recognized
                and there is other relations after it.
                Ex.: attribute__in__database_set

            PumpWoodQueryException(It is not possible to continue building
                                   query, underscore token ({token}) not found
                                   on model columns, relations or operations.
                                   Original query string:...)
                If a token (value separated by "__" is not recognized as
                neither relations, column and operation)
                Ex: attribute__description__last_update_at

            PumpWoodQueryException('Order value %s not implemented , sup and
                                   desc available, for column %s. Original
                                   query string %s')
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
        model_class_name = object_model.__class__.__name__
        join_models = []
        columns_values_filter = []
        for arg, value in query_dict.items():
            operation_key = None
            column = None
            json_key = None
            actual_model = object_model
            for token in arg.split('__'):
                # Check if it is to check a JSON key
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
                    # It is not possible to query for relations after
                    # specifying a column.
                    if column is not None:
                        template = "It is not permited more relations " + \
                            "after column underscore (%s). Original query " + \
                            "string (%s)"
                        raise PumpWoodQueryException(
                            template % (column.key, arg))

                    actual_model = relations[token][0]
                    join_models.append(relations[token])

                # Check if is search for primary_key
                elif token == 'pk': # NOQA
                    column = mapper.primary_key[0]

                # Check if is search for column
                elif token in columns.keys():
                    if column is not None:
                        template = "It is not permited more columns after " +\
                            "column underscore ({key}). Original query " + \
                            "string ({string})"
                        raise PumpWoodQueryException(
                            template, payload={
                                'key': column.key,
                                'string': arg})
                    column = columns[token]
                elif token in cls._underscore_operators.keys():
                    operation_key = token
                else:
                    msg = (
                        'It is not possible to continue building query, ' +
                        'underscore token ({token}) not found on model ' +
                        '[{model_name}] columns, relations or operations.' +
                        'Original query ' +
                        'string: "{query}".\n' +
                        'Columns: {cols}\n' +
                        'Relations: {rels}\n' +
                        'Operations: {opers}')
                    raise PumpWoodQueryException(
                        msg, payload={
                            'model_name': model_class_name,
                            'token': token, 'query': arg,
                            'cols': str(list(columns.keys())),
                            'rels': str(list(relations.keys())),
                            'opers': str(
                                list(cls._underscore_operators.keys()))
                        })

            if order:
                if value not in ['asc', 'desc']:
                    template = "Order value %s not implemented , sup and " + \
                        "desc available, for column %s. Original query " + \
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
                # operation_key is not set consider it a exact match
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
    def sqlalchemy_kward_query(cls, object_model, filter_dict: dict = {},
                               exclude_dict: dict = {},
                               order_by: list[str] = []):
        """Build SQLAlchemy engine string according to database parameters.

        Args:
            object_model:
                SQLAlchemy declarative model.
            filter_dict (dict):
                Dictionary to be used in filtering.
            exclude_dict (dict):
                Dictionary to be used in excluding.
            order_by (list[str]):
                Dictionary to be used as ordering.

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
        mapper = inspect(object_model.__table__)
        primary_keys = [
            col.name for col in list(mapper.c) if col.primary_key]
        if 1 < len(primary_keys):
            filter_dict = open_composite_pk(
                query_dict=filter_dict, is_filter=True)
            exclude_dict = open_composite_pk(
                query_dict=exclude_dict, is_filter=False)

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
            q = q.filter(~excl['operation'](excl['column'], excl['value']))
        # Order clauses
        for ord in order_query['columns']:
            q = q.order_by(ord['operation'](ord['column']))
        return q

    @classmethod
    def aggregate(cls, session, object_model,
                  query: flask_sqlalchemy.query.Query, group_by: list[str],
                  agg: dict, order_by: list[str] = []):
        """Aggregate results using group_by and agg.

        Args:
            session:
                Database session to perform query.
            object_model:
                SQLAlchemy declarative model.
            query (flask_sqlalchemy.query.Query):
                SQLAlchmy query that will be aggreted using group_by and
                agg parameters. Is is possible.
            group_by (list[str]):
                Group by columns that will used.
            agg (dict):
                Aggregation clauses that will be used at on query.
            order_by (str[str]):
                Columns to be used on ordering of the results from
                aggregation.
        """
        subquery = query.subquery()
        subquery_columns = dict([
            (col.key, col) for col in list(subquery.c)])

        # Creating 'Group By' statements
        model_group_by = []
        for g_col in group_by:
            temp_col = subquery_columns.get(g_col)
            if temp_col is not None:
                model_group_by.append(temp_col)
            else:
                msg = (
                    "Field [{field}] used on group by clause "
                    "not found on model [{model}]. Related model "
                    "fields are not implemented yet.")
                raise PumpWoodQueryException(
                    message=msg, payload={
                        'field': g_col, 'model': object_model.__name__})

        # Creating 'Aggregation Function' statements
        dict_orm_fun = {
            'sum': func.sum, 'mean': func.avg, 'count': func.count,
            'min': func.min, 'max': func.max, 'std': func.stddev_pop,
            'var': func.var_pop}
        model_agg = []
        for key, item in agg.items():
            # Validate if fields are correctly passed to aggregation
            # function.
            field = item.get('field')
            function = item.get('function')

            is_not_val_arg_type = (
                (type(field) is not str) or (type(function) is not str))
            if is_not_val_arg_type:
                msg = (
                    "agg key [{key}] field [{field}] or function "
                    "[{function}] are not strings or are None")
                raise PumpWoodQueryException(
                    message=msg, payload={
                        'key': key, 'field': field,
                        'function': function})

            django_orm_fun = dict_orm_fun.get(function)
            if django_orm_fun is None:
                msg = (
                    "agg key [{key}] function [{function}] is not implemented")
                raise PumpWoodNotImplementedError(
                    message=msg, payload={
                        'key': key, 'function': function})

            model_field = subquery_columns.get(field)
            if model_field is None:
                msg = (
                    "Field [{field}] used on aggregation function clause "
                    "[{key}] not found on model [{model}]. Related model "
                    "fields are not implemented yet.")
                raise PumpWoodQueryException(
                    message=msg, payload={
                        'field': field, 'model': object_model.__name__,
                        'key': key})
            agg_field = django_orm_fun(model_field).label(key)
            model_agg.append(agg_field)

        query_statments = model_group_by + model_agg
        result_query = session.query(*query_statments)\
            .group_by(*model_group_by)

        if len(order_by) == 0:
            # If order_by is lenght == 0 than it is not necessary create
            # a subquery for ordering
            return result_query
        else:
            # If order by arguments are passed to function, create a subquery
            # to order de results acording to columns, including the
            # one created with agg functions
            subquery_order_by = result_query.subquery()
            sub_order_by_col_dict = dict([
                (col.key, col) for col in list(subquery_order_by.c)])
            order_by_args = []
            for o in order_by:
                if type(o) is not str:
                    msg = (
                        "Order by arguments must be strings, "
                        "order_by [{order_by}]")
                    raise PumpWoodQueryException(
                        msg, payload={'order_by': order_by})

                # Descendent order will be indicated using - in a Django style
                order_type = 'asc'
                if o[0] == '-':
                    order_type = 'desc'
                    o = o[1:]

                model_column = sub_order_by_col_dict.get(o)
                if model_column is None:
                    msg = (
                        "Order by column [{column}] not found on query, "
                        "order_by [{order_by}]")
                    raise PumpWoodQueryException(
                        msg, payload={'column': o, 'order_by': order_by})

                if order_type == 'asc':
                    order_by_args.append(model_column)
                else:
                    order_by_args.append(desc(model_column))
            return session.query(subquery_order_by)\
                .order_by(*order_by_args)
