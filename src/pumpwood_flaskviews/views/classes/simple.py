"""Simple pumpwood view."""
import os
import io
import pandas as pd
import textwrap
import inspect
import datetime
import simplejson as json
from typing import Any, Union, List
from flask.views import View
from flask import request, Response
from flask import jsonify, send_file
from werkzeug.utils import secure_filename
from loguru import logger
from sqlalchemy import inspect as alchemy_inspect
from sqlalchemy_utils.types.choice import ChoiceType
from sqlalchemy.sql import text
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql.schema import Sequence, UniqueConstraint
from sqlalchemy.sql.expression import False_ as sql_false
from sqlalchemy.sql.expression import True_ as sql_true
from geoalchemy2.types import Geometry
from marshmallow import missing
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.cache import default_cache

# Flask view
from pumpwood_flaskviews.inspection import model_has_column
from pumpwood_flaskviews.query import SqlalchemyQueryMisc, BaseQuery
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.action import load_action_parameters
from pumpwood_flaskviews.config import INFO_CACHE_TIMEOUT
from pumpwood_i8n.singletons import pumpwood_i8n as _


class PumpWoodFlaskView(View):
    """PumpWoodFlaskView base view for pumpwood like models.

    A helper to build views that can be consumed using pumpwood
    microsservice.
    """

    _view_type = "simple"
    _primary_keys = None

    #####################
    # Route information #
    description = None
    dimensions = {}
    icon = None
    #####################

    CHUNK_SIZE = 4096

    # Database connection
    db = None

    # SQLAlchemy model
    model_class = None

    # Marshmellow serializer
    serializer = None

    # List of the fields that will be returned by default on list requests
    list_fields = None

    # Dict with the foreign key to other models, it does not ensure consistency
    # it will be avaiable on routes model and at fill_options for
    # documentation
    foreign_keys = {}
    relationships = {}

    # Set file fields that are on model, it is a dictionary with key as the
    # column name and values as lists of the extensions that will be permitted
    # on field, '*' will allow any type of file
    file_fields = {}

    # File path on storage will be '{model_class}__{field}/', setting this
    # attribute will change de behavior to '{file_folder}__{field}/'
    file_folder = None

    # PumpwoodStorage object
    storage_object = None

    # PumpwoodMicroservice object
    microservice = None

    # Set if save, delete, action end-point must trigger ETL Trigger on
    # request finish.
    trigger = True

    # Some large table are partitioned using one or more columns. Fetching
    # without setting this columns on filters may lead to reduced performance,
    # or even query hanging without finishing. If this variable is set it will
    # not allow flat_list_by_chunks queries on PumpwoodCommunication without
    # setting at least the first partition as equal or in filter.
    table_partition = []

    # Front-end uses 50 as limit to check if all data have been fetched,
    # if change this parameter, be sure to update front-end list component.
    list_paginate_limit = 50
    methods = ['GET', 'POST', 'DELETE', 'PUT']

    # List available micro services
    __last_available_microservices = None
    available_microservices = None

    # GUI attributes
    gui_retrieve_fieldset: dict = None
    gui_verbose_field: str = 'pk'
    gui_readonly: List[str] = []

    ########################
    # Get class attributes #
    def get_gui_retrieve_fieldset(self):
        """Return gui_retrieve_fieldset attribute."""
        # Set pk as verbose field if none is set
        return self.gui_retrieve_fieldset

    def get_gui_verbose_field(self):
        """Return gui_verbose_field attribute."""
        return self.gui_verbose_field

    def get_gui_readonly(self):
        """Return gui_readonly attribute."""
        return self.gui_readonly

    def get_list_fields(self):
        """Return list_fields attribute."""
        serializer_obj = self.serializer()
        return serializer_obj.get_list_fields()

    def base_query(self):
        """Create base query applying row permission filter.

        This function can be overided to add new funcionalities and filters
        to logged user data.

        Returns:
            Return a sqlalchemy query with applied base filters.
        """
        return BaseQuery\
            .row_permission_filter(model=self.model_class)

    @classmethod
    def _extract_primary_keys(cls,
                              dict_columns: dict[str | dict]) -> list[str]:
        """Extract primary keys from fields."""
        return [
            key for key, item in dict_columns.items()
            if item["primary_key"]]

    @classmethod
    def get_primary_keys(cls):
        """Return primary keys used on model at database.

        If class attribute `_primary_keys` is not set, `cls_fields_options()`
        function will run and cache `_primary_keys` on object attribute
        reducing the need of inspecting the database at each call of the
        function.
        """
        if cls._primary_keys is None:
            dict_columns = cls.cls_fields_options()
            cls._primary_keys = cls._extract_primary_keys(
                dict_columns=dict_columns)
        return cls._primary_keys

    def check_microservices(self, microservice: str) -> bool:
        """Check if microservice is avaiable.

        Args:
            microservice (str):
                Name of the microservice to check if is
                avaiable on Kong services.

        Return:
            True if microservice is avaiable o kong services.
        """
        list_microservices = self.get_available_microservices()
        return microservice in list_microservices

    def get_available_microservices(self) -> List[str]:
        """Get avaiable microservices.

        Args:
            No args.
        Kwargs:
            No kwargs.
        """
        hash_dict = {
            "context": "pumpwood_flaskviews",
            "end-point": "get_available_microservices"}
        cache_data = default_cache.get(hash_dict=hash_dict)
        if cache_data is not None:
            return cache_data
        else:
            available_microservices = \
                list(self.microservice.list_registered_routes().keys())
            default_cache.set(
                hash_dict=hash_dict,
                value=available_microservices,
                expire=INFO_CACHE_TIMEOUT)
            return available_microservices

    def get_session(self):
        """Ping connection before using database.

        Ping connection before quering database and restore session if
        necessary.
        """
        session = self.db.session
        try:
            session.execute(text("SELECT 1;"))
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        return session

    @classmethod
    def pumpwood_pk_get(cls, pk: Union[str, int]) -> object:
        """Get model_class object using pumpwood pk.

        Pumpwood pk may be integers and base64 strings coding a dictionary
        with composite primary keys. This function abstract SQLAlchemy
        query.get to treat both possibilities.

        Args:
            pk (str, int):
                Pumpwood primary key.

        Return:
            Returns a SQLAlchemy object with corresponding primary key.
        """
        converted_pk = CompositePkBase64Converter.load(pk)
        model_object = cls.model_class.query.get(converted_pk)
        return model_object

    @classmethod
    def create_route_object(cls, service_object: dict) -> dict:
        """Build Route object from view information.

        Creates a route object on admin microservice, which will register a
        route in Kong using service created by service_object.

        Args:
            service_object (dict:KongService):
                A serialized KongService object
                on which will be registred the new route.

        Returns:
            Returns a serialized object of KongRoute.
        """
        if service_object is not None:
            cls.microservice.login()
            serializer_obj = cls.serializer()

            model_class_name = cls.model_class.__name__
            suffix = os.getenv('ENDPOINT_SUFFIX', '')
            model_class_name = suffix + model_class_name

            route_url = '/rest/%s/' % model_class_name.lower()
            route_name = model_class_name.lower()
            notes = textwrap.dedent(cls.model_class.__doc__).strip()

            # Checking unique constraints
            unique_docs = ""
            table_args = getattr(cls.model_class, "__table_args_", [])
            for x in table_args:
                if isinstance(x, UniqueConstraint):
                    unique_columns = ", ".join([col.name for col in x.columns])
                    if unique_docs == "":
                        unique_docs += "\n\n- Unique Constraints:"
                    unique_docs += "\n[" + unique_columns + "]"
            notes = notes + unique_docs
            route_object = {
                "model_class": "KongRoute",
                "service_id": service_object["pk"],
                "route_url": route_url,
                "route_name": route_name,
                "route_type": "endpoint",
                "description": cls.description,
                "notes": notes,
                "dimensions": cls.dimensions,
                "icon": cls.icon,
                "extra_info": {
                    "view_type": cls._view_type,
                    "list_fields": serializer_obj.get_list_fields(),
                    "foreign_keys": serializer_obj.get_foreign_keys(),
                    "related_fields": serializer_obj.get_related_fields(),
                    "file_fields": cls.file_fields,
                    'gui_retrieve_fieldset': cls.gui_retrieve_fieldset,
                    'gui_verbose_field': cls.gui_verbose_field,
                    'gui_readonly': cls.gui_readonly}}
            try:
                cls.microservice.save(route_object)
            except Exception as e:
                msg = "Error when registering model [{model}]:\n{msg}".format(
                     model=model_class_name.lower(), msg=str(e))
                raise exceptions.PumpWoodOtherException(msg)

    @staticmethod
    def _allowed_extension(filename, allowed_extensions):
        extension = 'none'
        if '.' in filename:
            extension = filename.rsplit('.', 1)[1].lower()

        if "*" not in allowed_extensions:
            if extension not in allowed_extensions:
                return [(
                    "File '{filename}' with extension '{extension}' not " +
                    "allowed.\n Allowed extensions: {allowed_extensions}"
                ).format(filename=filename, extension=extension,
                         allowed_extensions=str(allowed_extensions))]
        return []

    def dispatch_request(self, end_point, first_arg=None, second_arg=None):
        """Dispatch request acordint o end_point, first_arg and second_arg."""
        # Force model to be init and avoid 'DeclarativeAttributeIntercept'

        AuthFactory.check_authorization(
            request_method=request.method.lower(),
            path=request.path, end_point=end_point,
            first_arg=first_arg, second_arg=second_arg,
            payload_text=request.get_data()[:300])

        # Extract data for post requests
        data = None
        if request.method.lower() in ('post', 'put'):
            if request.mimetype == 'application/json':
                data = request.get_json()
            else:
                data = request.form.to_dict()
                json_data_str = data.pop("__json__", '{}')
                try:
                    json_data = json.loads(json_data_str)
                except Exception:
                    msg = (
                        "'__json__' key is present at object data, but it " +
                        "was not possible to load its content.")
                    raise exceptions.PumpWoodWrongParameters(
                        message=msg, payload={"__json__": json_data_str})
                data.update(json_data)

        # List end-points
        if end_point == 'list' and request.method.lower() == 'post':
            endpoint_dict = data or {}
            return jsonify(self.list(**endpoint_dict))

        if end_point == 'list-without-pag' and \
           request.method.lower() == 'post':
            endpoint_dict = data or {}
            return jsonify(self.list_without_pag(**endpoint_dict))

        # Retrieve with list serializer
        if end_point == 'list-one':
            raise exceptions.PumpWoodNotImplementedError(
                'List one is deprected')

        # retrieve end-points
        if end_point == 'retrieve':
            if first_arg is None:
                return jsonify(self.object_template())

            if request.method.lower() == 'get':
                try:
                    fields = json.loads(
                        request.args.get('fields', 'null'))
                    foreign_key_fields = json.loads(
                        request.args.get('foreign_key_fields', 'false'))
                    related_fields = json.loads(
                        request.args.get('related_fields', 'false'))
                    default_fields = json.loads(
                        request.args.get('default_fields', 'false'))
                except Exception:
                    msg = (
                        "Error when deserializing url query parameters, "
                        "check parameters:\n"
                        "- fields [list]: Must be a list of strings\n"
                        "- foreign_key_fields [bool]\n"
                        "- related_fields [bool]\n"
                        "- default_fields [bool]")
                    raise exceptions.PumpWoodWrongParameters(msg)
                return jsonify(self.retrieve(
                    pk=first_arg, fields=fields,
                    foreign_key_fields=foreign_key_fields,
                    related_fields=related_fields,
                    default_fields=default_fields))

        if end_point == 'retrieve-file':
            if request.method.lower() == 'get':
                if first_arg is None:
                    raise exceptions.PumpWoodForbidden(
                        "To retrieve a file you must pass object pk.")

                file_field = request.args.get('file-field')
                if file_field is None:
                    raise exceptions.PumpWoodForbidden(
                        "To retrieve a file you must pass the file-field " +
                        "url argument.")
                file_data = self.retrieve_file(
                    pk=first_arg, file_field=file_field)

                return send_file(
                    io.BytesIO(file_data["data"]), as_attachment=True,
                    download_name=file_data["file_name"])

        if end_point == 'retrieve-file-streaming':
            if request.method.lower() == 'get':
                if first_arg is None:
                    raise exceptions.PumpWoodForbidden(
                        "To retrieve a file you must pass object pk.")

                file_field = request.args.get('file-field')
                if file_field is None:
                    raise exceptions.PumpWoodForbidden(
                        "To retrieve a file you must pass the file-field " +
                        "url argument.")
                file_iterator = self.retrieve_file_streaming(
                    pk=first_arg, file_field=file_field)

                return Response(
                    file_iterator, mimetype="application/octet-stream")

        # Save end-points
        if end_point == 'save' and request.method.lower() in ('post', 'put'):
            fields = json.loads(
                request.args.get('fields', 'null'))
            foreign_key_fields = json.loads(
                request.args.get('foreign_key_fields', 'false'))
            related_fields = json.loads(
                request.args.get('related_fields', 'false'))
            default_fields = json.loads(
                request.args.get('default_fields', 'false'))
            save_data = self.save(
                data=data, fields=fields,
                foreign_key_fields=foreign_key_fields,
                related_fields=related_fields,
                default_fields=default_fields)
            return jsonify(save_data)

        if end_point == "save-file-streaming" and \
                request.method.lower() in ('post', 'put'):
            if first_arg is None:
                raise exceptions.PumpWoodException(
                    "Save file stream endpoint have a pk")

            # Get URL parameters for the end-point
            fields = json.loads(
                request.args.get('fields', 'null'))
            foreign_key_fields = json.loads(
                request.args.get('foreign_key_fields', 'false'))
            related_fields = json.loads(
                request.args.get('related_fields', 'false'))
            default_fields = json.loads(
                request.args.get('default_fields', 'false'))
            file_field = request.args.get('file_field')
            if file_field is None:
                raise exceptions.PumpWoodForbidden(
                    "file_field not set as url parameter")
            file_name = request.args.get('file_name')
            save_streaming_data = self.save_file_streaming(
                pk=first_arg, file_field=file_field, file_name=file_name,
                fields=fields, foreign_key_fields=foreign_key_fields,
                related_fields=related_fields, default_fields=default_fields)
            return jsonify(save_streaming_data)

        if end_point == "remove-file-field" and \
                request.method.lower() in ('delete'):
            file_field = request.args.get('file-field')
            if file_field is None:
                raise exceptions.PumpWoodForbidden(
                    "file_field not set as url parameter")
            return jsonify(self.remove_file_field(
                pk=first_arg, file_field=file_field))

        # Delete end-point
        if end_point == 'delete':
            if request.method.lower() == 'delete':
                if first_arg is None:
                    raise exceptions.PumpWoodException(
                        "Delete endpoint with delete method must have a pk")
                force_delete = json.loads(
                    request.args.get('force_delete', 'false'))
                return jsonify(self.delete(
                    pk=first_arg, force_delete=force_delete))

            if request.method.lower() == 'post':
                endpoint_dict = data or {}
                return jsonify(self.delete_many(**endpoint_dict))

        # Actions end-points
        if end_point == 'actions':
            if request.method.lower() == 'get':
                return jsonify(self.list_actions())

            elif request.method.lower() == 'post':
                if first_arg is None:
                    return jsonify(
                        self.list_actions_with_objects(objects=data))
                else:
                    action_result = self.execute_action(
                        action_name=first_arg,
                        pk=second_arg, parameters=data)
                    result = action_result["result"]
                    if type(result) is dict:
                        result_keys = result.keys()
                        is_file_return = (
                            "__file_name__" in result_keys) and (
                            "__file__" in result_keys)
                        if is_file_return:
                            temp_result = action_result["result"]
                            return send_file(
                                temp_result["__file__"], as_attachment=True,
                                download_name=temp_result["__file_name__"])
                    return jsonify(action_result)

        # Options end-points
        if end_point == 'options':
            if request.method.lower() == 'get':
                return jsonify(self.search_options())

            if request.method.lower() == 'post':
                return jsonify(self.fill_options(
                    partial_data=data, field=first_arg))

        if end_point == 'list-options':
            if request.method.lower() == 'get':
                return jsonify(self.list_view_options())

        if end_point == 'retrieve-options':
            if request.method.lower() == 'get':
                return jsonify(self.retrieve_view_options())

            if request.method.lower() == 'post':
                user_type = request.args.get('user_type', 'api')
                field = request.args.get('field')
                resp = self.fill_options_validation(
                    partial_data=data, field=field,
                    user_type=user_type)
                return jsonify(resp)

        if end_point == 'aggregate':
            if request.method.lower() == 'post':
                endpoint_dict = data or {}
                return jsonify(self.aggregate(**endpoint_dict))

        raise exceptions.PumpWoodException(
            'End-point %s for method %s not implemented' % (
                end_point, request.method))

    @classmethod
    def as_view(cls, *class_args, **class_kwargs):
        """Set view name as model_class name."""
        return super(PumpWoodFlaskView, cls).as_view(
            name=cls.model_class.__name__, *class_args, **class_kwargs)

    def list(self, filter_dict: None | dict = None, exclude_dict: dict = None,
             order_by: list = None, fields: list = None,
             limit: int = None, default_fields: bool = False,
             foreign_key_fields: bool = False, **kwargs) -> list:
        """Return query result pagination.

        Args:
            filter_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by (list):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            fields (list):
                Fields to be returned.
            limit (int):
                Number of objects to be returned.
            default_fields (bool):
                Return the fields specified at self.list_fields.
            foreign_key_fields (bool):
                If foreign_key fields should be returned. This might take
                some time...
            **kwargs:
                Compatibylity and super the function.

        Returns:
            Return a list of serialized objects using self.serializer and
            filtered by args.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by
        self.get_session()

        # Do not display deleted objects
        if model_has_column(self.model_class, column='deleted'):
            info_msg = 'deleted field detected: model_class[{model_class}]'\
                .format(model_class=self.model_class.__name__)
            logger.info(info_msg)
            exclude_dict_keys = exclude_dict.keys()
            any_delete = False
            for key in exclude_dict_keys:
                first = key.split("__")[0]
                if first == "deleted":
                    any_delete = True
                    break
            if not any_delete:
                exclude_dict["deleted"] = True

        list_paginate_limit = limit or self.list_paginate_limit
        base_query = self.base_query()
        query_result = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=self.model_class,
                base_query=base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict,
                order_by=order_by)\
            .limit(list_paginate_limit).all()

        list_serializer = self.serializer(
            many=True, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=False)
        return list_serializer.dump(query_result, many=True)

    def list_without_pag(self, filter_dict: None | dict = None,
                         exclude_dict: dict = None, order_by: list = None,
                        fields: list = None, default_fields: bool = False,
                        foreign_key_fields: bool = False, **kwargs) -> list:
        """Return query without pagination.

        Args:
            No args.
        Kargs:
            filter_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict (dict):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by (list):
                Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            fields (list):
                Fields to be returned.
            default_fields (bool):
                Return the fields specified at self.list_fields.
            foreign_key_fields (bool):
                If foreign_key fields should be returned. This might take
                some time...
            **kwargs:
                Compatibylity with other versions and extension of class.

        Return:
            Return a list of serialized objects using self.serializer and
            filtered by args without pagination all values.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by
        self.get_session()

        # Do not display deleted objects
        if model_has_column(self.model_class, column='deleted'):
            info_msg = 'deleted field detected: model_class[{model_class}]'\
                .format(model_class=self.model_class.__name__)
            logger.info(info_msg)
            exclude_dict_keys = exclude_dict.keys()
            any_delete = False
            for key in exclude_dict_keys:
                first = key.split("__")[0]
                if first == "deleted":
                    any_delete = True
                    break
            if not any_delete:
                exclude_dict["deleted"] = True

        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict
        to_function_dict['order_by'] = order_by

        query_result = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            **to_function_dict).all()
        list_serializer = self.serializer(
            many=True, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=False)
        return list_serializer.dump(query_result, many=True)

    def retrieve(self, pk: Any, fields: list = None,
                 foreign_key_fields: bool = False,
                 related_fields: bool = False,
                 default_fields: bool = False) -> dict:
        """Retrieve object with pk.

        Args:
            pk (int | str):
                Primary key of the object to be returned.
            fields (list):
                Fields to be returned.
            default_fields (bool):
                Return the fields specified at self.list_fields.
            foreign_key_fields (bool):
                If foreign_key fields should be returned. This might take
                some time...
            related_fields (bool):
                If related fields (M2M) should be returned. This might take
                some time...

        Return:
            A dictionary with the serialized values of the object.
        """
        self.get_session()

        model_object = self.pumpwood_pk_get(pk=pk)
        if pk is not None and model_object is None:
            temp_model_class = self.model_class.__mapper__.class_.__name__
            # Try to convert pk to int for correct raising
            try:
                pk = int(pk)
            except Exception:
                pk = pk

            message = "Requested object {model_class}[{pk}] not found."
            raise exceptions.PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": temp_model_class,
                    "pk": pk})

        retrieve_serializer = self.serializer(
            many=False, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=related_fields)
        return retrieve_serializer.dump(model_object)

    def retrieve_file(self, pk: int | str, file_field: str):
        """Read file without stream.

        Args:
            pk (int | str):
                Pk of the object to save file field.
            file_field (str):
                File field to receive stream file.

        Returns:
            A stream of bytes with da file.
        """
        temp_model_class = self.model_class.__mapper__.class_.__name__
        if self.storage_object is None:
            raise exceptions.PumpWoodForbidden(
                "storage_object attribute not set for " +
                "model_class [{model_class}] view, file operations "
                "are disable", payload={
                    "model_class": temp_model_class})

        if file_field not in self.file_fields.keys():
            raise exceptions.PumpWoodForbidden(
                "file_field [{file_field}] must be set on self.file_fields " +
                "[{file_fields_keys}] dictionary at " +
                "model_class[{model_class}] view.", payload={
                    "file_field": file_field,
                    "file_fields_keys": list(self.file_fields.keys()),
                    "model_class": temp_model_class})

        object_data = self.retrieve(pk=pk)
        if file_field not in object_data.keys():
            raise exceptions.PumpWoodOtherException(
                "file_field [{file_field}] is not an attribute of "
                "model_class[{model_class}] ",
                payload={
                    "file_field": file_field,
                    "model_class": temp_model_class})

        file_path = object_data.get(file_field)
        if file_path is None:
            raise exceptions.PumpWoodObjectDoesNotExist(
                "file_field [{file_field}] is null at "
                "model_class[{model_class}]. File field is not set.",
                payload={
                    "file_field": file_field,
                    "model_class": temp_model_class})
        try:
            file_exists = self.storage_object.check_file_exists(file_path)
        except Exception as e:
            raise exceptions.PumpWoodOtherException(message=str(e))

        if not file_exists:
            msg = (
                "Object [{pk}] of model class [{model_class}] with "
                "file_field [{file_field}] not found in " +
                "storage path [{file_path}].")
            raise exceptions.PumpWoodObjectDoesNotExist(
                message=msg, payload={
                    "model_class": temp_model_class,
                    "pk": object_data["pk"],
                    "file_field": file_field,
                    "file_path": file_path})

        file_data = self.storage_object.read_file(file_path)
        file_name = os.path.basename(file_path)
        file_data["file_name"] = file_name
        return file_data

    def retrieve_file_streaming(self, pk: int | str, file_field: str):
        """Read file using stream.

        Args:
            pk (int | str):
                Pk of the object to save file field.
            file_field(str):
                File field to receive stream file.

        Returns:
            A stream of bytes with da file.
        """
        if self.storage_object is None:
            raise exceptions.PumpWoodForbidden(
                "storage_object attribute not set for view, file operations "
                "are disable")

        if file_field not in self.file_fields.keys():
            raise exceptions.PumpWoodForbidden(
                "file_field must be set on self.file_fields dictionary.")
        object_data = self.retrieve(pk=pk)

        file_path = object_data.get(file_field)
        if file_path is None:
            raise exceptions.PumpWoodObjectDoesNotExist(
                "field [{}] not found or null at object".format(file_field))

        try:
            file_exists = self.storage_object.check_file_exists(file_path)
        except Exception as e:
            raise exceptions.PumpWoodException(message=str(e))

        if not file_exists:
            msg = (
                "Object not found in storage [{}]").format(file_path)
            raise exceptions.PumpWoodObjectDoesNotExist(
                message=msg, payload={
                    "model_class": self.model_class.__mapper__.class_.__name__,
                    "pk": object_data["pk"],
                    "file_path": file_path})

        return self.storage_object.get_read_file_iterator(file_path)

    def remove_file_field(self, pk: int | str, file_field: str) -> bool:
        """Remove file field.

        Args:
            pk (int | str):
                pk of the object.
            file_field (str):
                name of the file field.

        Raises:
            PumpWoodForbidden: If file_file is not in file_fields keys of the
                view.
            PumpWoodException: Propagates exceptions from storage_objects.
        """
        if self.storage_object is None:
            raise exceptions.PumpWoodForbidden(
                "storage_object attribute not set for view, file operations "
                "are disable")

        if file_field not in self.file_fields.keys():
            raise exceptions.PumpWoodForbidden(
                "file_field must be set on self.file_fields dictionary.")

        session = self.get_session()
        obj = self.pumpwood_pk_get(pk=pk)
        file_path = file_path = getattr(obj, file_field)
        if file_path is None:
            raise exceptions.PumpWoodObjectDoesNotExist(
                "File does not exist. File field [{}] is set as None".format(
                    file_field))

        setattr(obj, file_field, None)
        session.add(obj)
        session.commit()

        try:
            self.storage_object.delete_file(file_path)
            return True
        except Exception as e:
            raise exceptions.PumpWoodException(message=str(e))

    def object_template(self):
        """Return an empty object to be used as template to create new one."""
        empty_object = self.model_class()
        retrieve_serializer = self.serializer(many=False)
        return retrieve_serializer.dump(empty_object)

    def delete(self, pk: int | str, force_delete: bool = False) -> dict:
        """Delete object.

        If object have deleted as field, this function will not delete
        the object and will set the field to True.

        Setting force_delete to True will disable this feature and delete the
        object even with deleted field.

        Args:
            pk (int | str):
                Pk of the object to be deleted.
            force_delete (bool):
                If True force delete of the object even if object has deleted
                as field.
        Return [dict]:
            Return the excluded object.
        """
        session = self.get_session()
        model_object = self.pumpwood_pk_get(pk=pk)
        if pk is not None and model_object is None:
            message = "Requested object {model_class}[{pk}] not found.".format(
                model_class=self.model_class.__mapper__.class_.__name__, pk=pk)
            try:
                pk = int(pk)
            except Exception:
                pk = pk

            raise exceptions.PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": self.model_class.__mapper__.class_.__name__,
                    "pk": pk})

        temp_serializer = self.serializer(many=False)
        object_dump = temp_serializer.dump(model_object, many=False)

        # Remove deleted entries from results
        has_deleted = model_has_column(self.model_class, column='deleted')
        if has_deleted and not force_delete:
            model_object.deleted = True
            session.add(model_object)
            session.commit()
        else:
            try:
                session.delete(model_object)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

        available_microservices = self.get_available_microservices()
        pumpwood_etl_ok = 'pumpwood-etl-app' in available_microservices
        if self.microservice is not None and self.trigger and pumpwood_etl_ok:
            # Process ETLTrigger for the model class
            self.microservice.login()
            self.microservice.execute_action(
                "ETLTrigger", action="process_triggers", parameters={
                    "model_class": self.model_class.__name__.lower(),
                    "type": "delete",
                    "pk": object_dump["pk"],
                    "action_name": None})
        return object_dump

    def delete_many(self, filter_dict: dict = {},
                    exclude_dict: dict = {}) -> bool:
        """Delete many objects using query dictionaries as filter.

        Args:
            filter_dict (dict):
                Filter objects that will be deleted.
            exclude_dict (dict):
                Exclude objects that will be deleted.

        Returns:
            Return True.
        """
        session = self.get_session()
        try:
            to_function_dict = {}
            to_function_dict['object_model'] = self.model_class
            to_function_dict['filter_dict'] = filter_dict
            to_function_dict['exclude_dict'] = exclude_dict
            query_result = SqlalchemyQueryMisc.sqlalchemy_kward_query(
                **to_function_dict)
            query_result.delete(synchronize_session='fetch')
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        return True

    def save(self, data: dict, file_paths: dict = {},
             foreign_key_fields: bool = False, related_fields: bool = False,
             default_fields: bool = False, fields: list = None) -> dict:
        """Update object or save new object.

        Args:
            data (dict):
                Data used to save information on Pumpwood.
            file_paths (dict):
                Used when saving files with streaming, it is not
                exposed to save API. If will set the file path directly on
                the object.
            fields (list):
                Set fields to be returned at serializer.
            foreign_key_fields (bool):
                If foreign fields should be returned at the object serializer.
            related_fields (bool):
                If related fields should be returned at the object serializer.
            default_fields (bool):
                If default fields should be returned at the object serializer.

        Returns:
            Returns a dictonary with serialized object.
        """
        retrieve_serializer = self.serializer(
            many=False, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=related_fields)
        retrieve_serializer.context['authorization_token'] = \
            request.headers.get('Authorization', None)

        session = self.get_session()

        ##################################################################
        # Remove all fields that are files so it can be only loaded when #
        # a file is passed, ["!path!"] indicates that path is passed, but
        # it can be treated as file for downloading and interface
        file_fields_not_path = {}
        for key, item in self.file_fields.items():
            if item != ["!path!"]:
                data.pop(key, None)
                file_fields_not_path[key] = item

        pk = data.pop('pk', None)
        to_save_obj = None
        model_object = None
        # Retrieve object if pk is set
        if pk is not None:
            model_object = self.pumpwood_pk_get(pk=pk)
        to_save_obj = retrieve_serializer.load(
            data, instance=model_object, session=session)
        try:
            # Flush object to receive it's id to create file name,
            # but does not commit so if there is file errors it won't
            # persist on database.
            session.add(to_save_obj)
            session.flush()
        except Exception as e:
            session.rollback()
            raise e

        # Set file names with file_paths dict which is not exposed to API
        # this is only used by save_file_streaming to set file name
        for key, path in file_paths.items():
            setattr(to_save_obj, key, path)

        # True if files were added to the object
        with_files = False
        file_save_time = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%Hh%Mm%Ss")
        file_upload_errors = {}
        for field in file_fields_not_path.keys():
            field_errors = []
            if field in request.files:
                files_list = request.files.getlist(field)
                file_obj = None
                full_filename = None

                # Check if storage object was set
                if self.storage_object is None:
                    msg = (
                        "storage_object attribute not set for view, "
                        "file operations are disable")
                    field_errors.append(msg)

                # Check if only one files was uploaded for the file field
                # more than one file is not implemented
                if len(files_list) != 1:
                    msg = "More than one file passed."
                    field_errors.append(msg)

                # If one file was uploaded, check if the file extension of
                # the uploaded file is allowed
                else:
                    file_obj = files_list[0]
                    filename = secure_filename(file_obj.filename)
                    allowed_extension_errors = self._allowed_extension(
                        filename=filename,
                        allowed_extensions=self.file_fields[field])

                    # Check if _allowed_extension return errors
                    if len(allowed_extension_errors) != 0:
                        field_errors.extend(allowed_extension_errors)
                    else:
                        full_filename = "{}___{}___{}".format(
                            str(to_save_obj.id).zfill(15),
                            file_save_time,
                            filename)

                # Verify if there is any errors on file upload
                if len(field_errors) != 0:
                    file_upload_errors[field] = field_errors
                else:
                    # Set file folder path not using model_class
                    # if self.file_folder is set, useful if file
                    # is passed to other class using path.
                    model_class = self.model_class.__name__.lower()
                    if self.file_folder is not None:
                        model_class = self.file_folder
                    file_path = '{model_class}__{field}/'.format(
                        model_class=model_class, field=field)

                    # Save file on storage
                    storage_filepath = \
                        self.storage_object.write_file(
                            file_path=file_path,
                            file_name=full_filename,
                            data=file_obj.read(),
                            content_type=file_obj.content_type,
                            if_exists='overwrite')
                    setattr(
                        to_save_obj, field,
                        storage_filepath)

                    # Get hash if there is a {field}_hash on
                    # object attributes
                    field_hash = "{}_hash".format(field)
                    if hasattr(to_save_obj, field_hash):
                        file_hash = \
                            self.storage_object.get_file_hash(
                                file_path=storage_filepath)
                        setattr(
                            to_save_obj, field_hash,
                            file_hash)

                    # Mark that a file has been added to object and save
                    # the path latter.
                    with_files = True

        # Verify if the is any error when uploading files to storage
        if file_upload_errors:
            message = "Error when uploading files"
            payload = file_upload_errors
            session.rollback()
            raise exceptions.PumpWoodObjectSavingException(
                message=message, payload=payload)

        # If with files, update object on database to have uploaded file
        # paths
        if with_files:
            session.add(to_save_obj)

        # Commit file changes to database and persist object with file
        # information if present.
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

        # Serialize object to return
        result = retrieve_serializer.dump(to_save_obj)

        ###################################
        # Pumpwood ETLTrigger integration #
        available_microservices = self.get_available_microservices()
        pumpwood_etl_ok = 'pumpwood-etl-app' in available_microservices
        if self.microservice is not None and self.trigger and pumpwood_etl_ok:
            # Process ETL Trigger for the model class
            self.microservice.login()
            if pk is None:
                self.microservice.execute_action(
                    "ETLTrigger", action="process_triggers", parameters={
                        "model_class": self.model_class.__name__.lower(),
                        "type": "create",
                        "pk": None,
                        "action_name": None})
            else:
                self.microservice.execute_action(
                    "ETLTrigger", action="process_triggers", parameters={
                        "model_class": self.model_class.__name__.lower(),
                        "type": "update",
                        "pk": result["pk"],
                        "action_name": None})
        return result

    def save_file_streaming(self, pk: int | str, file_field: str,
                            file_name: str = None,
                            foreign_key_fields: bool = False,
                            related_fields: bool = False,
                            default_fields: bool = False,
                            fields: list = None) -> dict:
        """Save file to object.

        Args:
            pk (int | str):
                Pk of the object to be updated.
            file_field (str):
                Name of the file field in the object.
            file_name (str):
                File name that will be set for file streaming.
            fields (list):
                Set fields to be returned at serializer.
            foreign_key_fields (bool):
                If foreign fields should be returned at the object serializer.
            related_fields (bool):
                If related fields should be returned at the object serializer.
            default_fields (bool):
                If default fields should be returned at the object serializer.

        Returns:
            Serialized object.
        """
        if file_field not in self.file_fields.keys():
            raise exceptions.PumpWoodForbidden(
                "file_field must be set on self.file_fields dictionary.")
        file_field_extention = self.file_fields[file_field]

        if file_name is not None:
            file_name = secure_filename(file_name)
        else:
            extention = file_field_extention[0]
            file_name = file_field + "." + extention \
                if extention != "*" else file_field

        model_class = self.model_class.__name__.lower()
        file_path = '{model_class}__{field}/'.format(
            model_class=model_class, field=file_field)

        file_save_time = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%Hh%Mm%Ss")
        file_name = "{pk}___{time}___{filename}".format(
            pk=str(pk).zfill(15), time=file_save_time,
            filename=file_name)

        upload_response = self.storage_object.write_file_stream(
            file_path=file_path, file_name=file_name,
            data_stream=request.stream)

        object_data = self.retrieve(pk=pk)
        file_paths = {}
        file_paths[file_field] = upload_response["file_path"]
        self.save(data=object_data, file_paths=file_paths)
        return upload_response

    def get_actions(self):
        """Get all actions with action decorator."""
        # this import works here only
        function_dict = dict(inspect.getmembers(
            self.model_class, predicate=inspect.isfunction))
        method_dict = dict(inspect.getmembers(
            self.model_class, predicate=inspect.ismethod))
        method_dict.update(function_dict)
        actions = {
            name: func for name,
            func in method_dict.items() if getattr(func, 'is_action', False)
        }
        return actions

    def list_actions(self):
        """List model exposed actions."""
        actions = self.get_actions()
        action_descriptions = []
        model_class = self.model_class.__name__

        # Use I8n object to translate
        translation_tag_template = "{model_class}__action__{action}"
        for name, action in actions.items():
            action_dict = action.action_object.to_dict()
            tag = translation_tag_template.format(
                model_class=model_class, action=action_dict["action_name"])

            #########################################################
            # Translate action_name and info to end-user at verbose #
            action_dict["action_name__verbose"] = _.t(
                sentence=action_dict["action_name"], tag=tag + "__action_name")
            action_dict["info__verbose"] = _.t(
                sentence=action_dict["info"], tag=tag + "__info")
            for key, item in action_dict["parameters"].items():
                item["verbose_name"] = _.t(
                    sentence=key, tag=tag + "__parameters")
            #########################################################

            action_descriptions.append(action_dict)
        return action_descriptions

    def list_actions_with_objects(self, objects):
        """List model exposed actions acording to selected objects."""
        actions = self.get_actions()
        action_descriptions = [
            action.action_object.description
            for name, action in actions.items()]
        return action_descriptions

    def execute_action(self, action_name, pk=None, parameters={}):
        """Execute action over object or class using parameters."""
        self.get_session()

        actions = self.get_actions()
        rest_action_names = list(actions.keys())

        if action_name not in rest_action_names:
            message = (
                "There is no method {action} in rest actions " +
                "for {class_name}").format(
                action=action_name,
                class_name=self.model_class.__mapper__.class_.__name__)
            raise exceptions.PumpWoodException(
                message=message, payload={"action_name": action_name})

        action_fun = getattr(self.model_class, action_name)
        action_object = action_fun.action_object.to_dict()

        # If function is static and check if pk was passed to retrieve
        # object from database
        object_dict = None
        if pk is None:
            if not action_object["is_static_function"]:
                raise exceptions.PumpWoodActionArgsException(
                    "Function is not static and pk is Null")
        else:
            # If is not a static function, get the object associated with pk
            # them use it to retrieve the function associated with object,
            # this will inject self as argument of the function.
            if action_object["is_static_function"]:
                raise exceptions.PumpWoodActionArgsException(
                    "Function is static and pk is not Null")

            model_object = self.pumpwood_pk_get(pk=pk)
            if model_object is None:
                message_template = (
                    "Requested object {model_class}[{pk}] not found.")
                temp_model_class = self.model_class.__mapper__.class_.__name__
                raise exceptions.PumpWoodObjectDoesNotExist(
                    message=message_template, payload={
                        "model_class": temp_model_class,
                        "pk": pk})

            # Retrieve function associated with action to inject the object as
            # self parameter
            action_fun = getattr(model_object, action_name)

            # Create a serializer to serialize the object to return the value
            # at the action call
            temp_serializer = self.serializer(
                many=False, only=self.list_fields)
            object_dict = temp_serializer.dump(model_object, many=False)

        loaded_parameters = load_action_parameters(action_fun, parameters)
        result = action_fun(**loaded_parameters)

        available_microservices = self.get_available_microservices()
        pumpwood_etl_ok = 'pumpwood-etl-app' in available_microservices
        if self.microservice is not None and self.trigger and pumpwood_etl_ok:
            self.microservice.login()
            self.microservice.execute_action(
                "ETLTrigger", action="process_triggers", parameters={
                    "model_class": self.model_class.__name__.lower(),
                    "type": "action", "pk": pk, "action_name": action_name})

        return {
            'result': result, 'action': action_name,
            'parameters': parameters, 'object': object_dict}

    def aggregate(self, group_by: List[str], agg: dict,
                  filter_dict: dict = {}, exclude_dict: dict = {},
                  order_by: List[str] = [], limit: int = None,
                  format: str = 'list', **kwargs):
        """Aggregate database information using group_by.

        Args:
            group_by (list[str]):
                Columns that will be used at group by clause.
            agg (dict):
                Aggregation dictonary, each item will be converted on
                an aggregation function. The key will be the return column,
                field the field over which the aggregation function will
                be calculated.
            filter_dict (dict):
                Filter dictonary to be applied before aggregation.
            exclude_dict (dict):
                Exclude dictionary to be applied before aggregation.
            order_by (list[str]):
                Order by to order results from aggregation. Aggregation
                results (agg dictonary keys) can also be used when ordering.
            limit (int):
                Limit values return by the query.
            format (str):
                Format to be used in pivot, same argument used in
                pandas to_dict.
            **kwargs:
                Extra arguments not mapped yet.
        """
        session = self.get_session()

        # Do not display deleted objects
        if model_has_column(self.model_class, column='deleted'):
            exclude_dict_keys = exclude_dict.keys()
            any_delete = False
            for key in exclude_dict_keys:
                first = key.split("__")[0]
                if first == "deleted":
                    any_delete = True
                    break
            if not any_delete:
                exclude_dict["deleted"] = True

        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict

        subquery_result = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            object_model=self.model_class, filter_dict=filter_dict,
            exclude_dict=exclude_dict)
        pd_results = pd.DataFrame(
            SqlalchemyQueryMisc.aggregate(
                session=session, object_model=self.model_class,
                query=subquery_result, group_by=group_by,
                agg=agg, order_by=order_by).limit(limit).all())
        return pd_results.to_dict(format)

    @classmethod
    def cls_fields_options(cls):
        """Return description of the model fields."""
        # Get information from cache if avaiable
        hash_dict = {
            "context": "pumpwood_flaskviews",
            "end-point": "cls_fields_options",
            "model_class": cls.model_class.__name__}
        cache_data = default_cache.get(hash_dict=hash_dict)
        if cache_data is not None:
            return cache_data

        mapper = alchemy_inspect(cls.model_class)
        serializer_obj = cls.serializer()
        serializer_fields = serializer_obj.fields

        # Getting table/class map
        table_class_map = {}
        for mapper_temp in cls.db.Model.registry.mappers:
            model_name = mapper_temp.class_.__name__
            table_name = mapper_temp.persist_selectable.name
            table_class_map[table_name] = model_name

        dict_columns = {}
        model_class = cls.model_class.__name__
        translation_tag_template = "{model_class}__fields__{field}"
        for x in mapper.columns:
            column = x.name
            type_str = None
            if isinstance(x.type, Geometry):
                type_str = "geometry"
            else:
                type_str = x.type.python_type.__name__

            # If serializer is set, use serializer information. It will
            # overide database behavior (serializer's validation and defult
            # are set before database)
            temp_field = serializer_fields.get(column)
            read_only = False
            nullable = False
            default = None
            ser_field_default = None
            if temp_field is not None:
                dump_only = getattr(
                    temp_field, 'dump_only', False)
                # Custom attribute to help with calculated custom fields on
                # pumpwood
                pumpwood_read_only = getattr(
                    temp_field, 'pumpwood_read_only', False)
                read_only = dump_only or pumpwood_read_only

                nullable = getattr(temp_field, 'allow_none')
                ser_field_default = getattr(temp_field, 'dump_default')
                ser_field_default = (
                    None if ser_field_default is missing
                    else ser_field_default)
            else:
                nullable = x.nullable

            column_default = x.default
            if ser_field_default is not None:
                default = ser_field_default
            elif column_default is not None:
                arg = getattr(x.default, 'arg', None)
                if isinstance(arg, GenericFunction):
                    default = arg.description
                elif arg is dict:
                    default = {}
                elif inspect.isfunction(arg):
                    default = arg.__name__ + "()"
                elif isinstance(x.default, Sequence):
                    default = "#autoincrement#"
                elif isinstance(arg, sql_false):
                    default = False
                elif isinstance(arg, sql_true):
                    default = True
                else:
                    default = arg
            elif x.server_default is not None:
                arg = getattr(x.server_default, 'arg', None)
                if isinstance(arg, GenericFunction):
                    default = arg.description
                elif arg is dict:
                    default = {}
                elif inspect.isfunction(arg):
                    default = arg.__name__ + "()"
                elif isinstance(x.server_default, Sequence):
                    default = "#autoincrement#"
                elif isinstance(arg, sql_false):
                    default = False
                elif isinstance(arg, sql_true):
                    default = True
                else:
                    default = arg

            help_text = x.doc
            tag = translation_tag_template.format(
                model_class=model_class, field=column)
            column__verbose = _.t(
                sentence=column, tag=tag + "__column")
            help_text__verbose = _.t(
                sentence=help_text, tag=tag + "__help_text")
            column_info = {
                "primary_key": x.primary_key,
                "column": column,
                "column__verbose": column__verbose,
                "help_text": help_text,
                "help_text__verbose": help_text__verbose,
                "type": type_str,
                "nullable": nullable,
                "read_only": read_only,
                "default": default,
                "unique": x.unique,
                "extra_info": {}
            }

            if isinstance(x.type, ChoiceType):
                column_info["type"] = "options"
                in_dict = {}
                for choice in x.type.choices:
                    in_dict[choice[0]] = {
                        "description__verbose": _.t(
                            sentence=choice[1],
                            tag=tag + "__choice__" + choice[0]),
                        "description": choice[1]}
                column_info["in"] = in_dict

            if column_info["column"] == "id":
                column_info["default"] = "#autoincrement#"
                column_info["doc_string"] = "autoincrement id"

            file_field = cls.file_fields.get(x.name)
            if file_field is not None:
                column_info["type"] = "file"
                column_info["permited_file_types"] = file_field
            dict_columns[column_info["column"]] = column_info

        #######################################################
        # Modifying column types associated with foreign keys #
        # foreign_key dictionary will pass to front-end information to
        # render.
        serializer_obj = cls.serializer()
        foreign_keys = serializer_obj.get_foreign_keys()
        for field_name, field_extra_info in foreign_keys.items():
            tag = translation_tag_template.format(
                model_class=model_class, field=field_name)
            field_info = dict_columns.get(field_name)
            if field_info is None:
                msg = (
                    "foreign_key[{field}] not correctly configured for "
                    "model_class[{model_class}].")
                raise exceptions.PumpWoodOtherException(
                    msg, payload={
                        "field": field_name, "model_class": model_class})
            field_info["type"] = "foreign_key"
            field_info['extra_info'] = field_extra_info
            dict_columns[field_name] = field_info

        related_fields = serializer_obj.get_related_fields()
        for field_name, field_extra_info in related_fields.items():
            tag = translation_tag_template.format(
                model_class=model_class, field=field_name)
            column__verbose = _.t(
                sentence=field_name, tag=tag + "__related_field")
            field = serializer_obj._declared_fields[field_name]
            help_text__verbose = _.t(
                sentence=field.help_text, tag=tag + "__help_text")
            dict_columns[field_name] = {
                "primary_key": False,
                "column": field_name,
                "column__verbose": column__verbose,
                "help_text": field.help_text,
                "help_text__verbose": help_text__verbose,
                "type": "related_model",
                "nullable": False,
                "read_only": field.read_only,
                "default": None,
                "unique": False,
                "extra_info": field_extra_info}

        ############################################################
        # Stores primary keys as attribute to help other functions #
        primary_keys = cls._extract_primary_keys(
            dict_columns=dict_columns)
        help_text = (
            "table primary key" if len(primary_keys) == 1
            else "base64 encoded json dictionary")
        tag = translation_tag_template.format(
            model_class=model_class, field='pk')
        column__verbose = _.t(
            sentence='pk', tag=tag + "__column")
        help_text__verbose = _.t(
            sentence=help_text, tag=tag + "__help_text")

        dict_columns["pk"] = {
            "primary_key": True,
            "partition": cls.table_partition,
            "column": primary_keys,
            "column__verbose": column__verbose,
            "help_text": help_text,
            "help_text__verbose": help_text__verbose,
            "type": "#autoincrement#",
            "nullable": True,
            "read_only": True,
            "default": "#autoincrement#",
            "unique": True}

        # Set cache to reduce response time
        default_cache.set(
            hash_dict=hash_dict,
            value=dict_columns,
            expire=INFO_CACHE_TIMEOUT)
        return dict_columns

    def search_options(self):
        """# DEPRECTED # Return search options for list pages."""
        return self.cls_fields_options()

    def fill_options(self, partial_data, field=None):
        """# DEPRECTED # Return fill options for retrieve/save pages."""
        return self.cls_fields_options()

    def list_view_options(self) -> dict:
        """Return information to render list views on frontend.

        Args:
            No args.

        Return:
            Return a dictionary with keys:
            - list_fields[List[str]]: Return a list of fields that should be
                redendered on list view.
            - field_type [dict]: Return information for each column to
                render search filters on frontend.
        """
        list_fields = self.get_list_fields()
        fields_options = self.cls_fields_options()
        return {
            "default_list_fields": list_fields,
            "field_descriptions": fields_options}

    def retrieve_view_options(self) -> dict:
        """Return information to correctly create retrieve view.

        Field set are set using gui_retrieve_fieldset attribute of the
        class. It is used classes to define each fieldset.

        Args:
            No Args.
        Return [dict]:
            Return a dictonary with information to render retrieve
            views on front-ends. Keys:
             - fieldset [dict]: A dictionary with inline tabs names as
                key and fields that will be redendered.

            Exemple:
            {
                "fieldset": {
                    "Nome da tab. 1": {
                        "fields": ["field1", "field2", "field3"]
                    },
                    "Nome da tab. 2": {
                        "fields": ["field1"]
                    }
                }
            }
        """
        gui_retrieve_fieldset = self.get_gui_retrieve_fieldset()
        gui_verbose_field = self.get_gui_verbose_field()

        # If gui_retrieve_fieldset is not set return all columns
        # on the main tab
        if gui_retrieve_fieldset is None:
            fields_options = self.cls_fields_options()
            all_columns = set(fields_options.keys())
            all_columns = list(all_columns - {'pk', 'model_class'})
            all_columns.sort()
            return {
                "verbose_field": gui_verbose_field,
                "fieldset": {
                    None: {
                        "fields": all_columns
                    }
                }
            }
        return {
            "verbose_field": gui_verbose_field,
            "fieldset": gui_retrieve_fieldset}

    def fill_options_validation(self, partial_data: dict,
                                user_type: str = 'api',
                                field: str = None) -> dict:
        """Return fill options for retrieve/save pages.

        It will validate partial data fill and return erros if necessary.

        Args:
            partial_data (dict):
                Partially filled data to be validated by the backend.
            user_type (str):
                Must be in ['api', 'gui']. It will return the
                options according to interface user is using. When requesting
                using gui, self.gui_readonly field will be setted as read-only.
            field (str):
                Set to validade an specific field. If not set all
                fields will be validated.

        Returns:
            Return a dictionary.
        """
        gui_readonly = self.get_gui_readonly()
        fill_options = self.cls_fields_options()

        # If it is gui interface then set gui_readonly as read-only
        # this will limit fields that are not read-only but should not
        # be edited be the user
        if user_type == 'gui':
            for key, item in fill_options.items():
                if key in gui_readonly:
                    item["read_only"] = True
        return {
            "field_descriptions": fill_options,
            "gui_readonly": gui_readonly
        }
