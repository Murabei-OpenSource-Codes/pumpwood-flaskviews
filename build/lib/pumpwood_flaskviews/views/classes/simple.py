"""Simple pumpwood view."""
import os
import io
import pandas as pd
import textwrap
import inspect
import datetime
import simplejson as json
from typing import Any, Union, List, Literal
from loguru import logger
from flask.views import View
from flask import request, Response
from flask import jsonify, send_file
from werkzeug.utils import secure_filename
from flask_sqlalchemy.query import Query
from sqlalchemy.sql.schema import UniqueConstraint
from pumpwood_communication import exceptions
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_communication.cache import default_cache
from pumpwood_flaskviews.sqlalchemy import get_session
from pumpwood_flaskviews.exceptions import PumpWoodFlaskViewEndPointFoundError

# Flask view
from pumpwood_flaskviews.views.classes.aux import AuxFillOptions
from pumpwood_flaskviews.inspection import model_has_column
from pumpwood_flaskviews.query import SqlalchemyQueryMisc
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.action import LoadActionParameters
from pumpwood_flaskviews.config import INFO_CACHE_TIMEOUT
from pumpwood_i8n.singletons import pumpwood_i8n as _


class PumpWoodFlaskView(View):
    """Base view for PumpWood-like models.

    A helper class used to build views that are compatible with the
    PumpWood microservice ecosystem.
    """

    methods = ['GET', 'POST', 'DELETE', 'PUT']
    """Methods allowed on flask view."""

    CHUNK_SIZE: int = 4096
    """File upload/download chunk size."""

    _view_type = "simple"
    """Type of the view, this will be used to mark route type on route
       registration at auth app."""
    _primary_keys: list[str] = None
    """This field will be populated at the route registration, it will inspect
       object and verify if primary key is composite."""

    # These attribute must be set on class definition
    description: str = None
    """Description used to register route at auth service."""
    dimensions: dict[str, str] = {}
    """Dimensions used to tag route at auth service."""
    db = None
    """Database connection."""
    model_class = None
    """SQLAlchemy model."""
    serializer = None
    """Marshmellow serializer used at model end-points."""
    storage_object = None
    """PumpwoodStorage object, if not set file end-point will be disabled and
       will raise an error."""
    microservice: PumpWoodMicroService = None
    """PumpwoodMicroservice object."""

    # Optional attributes
    icon: str = None
    """Icon name that may be used on frontend for service routes."""
    file_fields: dict[str, list[str]] = {}
    """Fields that are considered files.

    Set file fields that are on model, it is a dictionary with key as the
    column name and values as lists of the extensions that will be permitted
    on field, '*' will allow any type of file
    """
    file_folder: str = None
    """File path on storage will be '{model_class}__{field}/', setting this
       attribute will change de behavior to '{file_folder}__{field}/'"""
    broadcast: bool = True
    """Set if save, delete, action end-points will broadcast to ETL
       microservice if present."""

    # Query information
    table_partition: list[str] = []
    """Specifies the table partitions applied to the database.

    Fetching data without applying these columns as filters may lead to
    severely reduced performance or query timeouts.
    """
    list_paginate_limit: int = 50
    """Front-end uses 50 as limit to check if all data have been fetched,
       if change this parameter, be sure to update front-end list component."""

    # GUI attributes
    gui_retrieve_fieldset: dict = None
    """Help front end to build field sets using a dictionary and lists."""
    gui_verbose_field: str = 'pk'
    """Sugest a verbose string to represent the object to user."""
    gui_readonly: List[str] = []
    """Fields that as set as read only on fill options for gui user, but are
       not set as read-only at serializer. Normally this are fields that are
       set at internal system process such as async calls."""

    # Get class attributes
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

    @classmethod
    def _add_default_filter(cls, query: Query = None):
        """Add class default filters to a SQLAlchemy query.

        Uses the base query object of the model to apply mandatory
        filters, such as row-level permissions or ownership restrictions.

        Args:
            query (Query):
                The initial SQLAlchemy query.

        Returns:
            Query:
                The query with applied default filters.
        """
        return cls.model_class.default_filter_query(query=query)

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
        """Check if a microservice is available in the current environment.

        Args:
            microservice (str):
                Name of the microservice to check on Kong services.

        Returns:
            bool:
                True if the microservice is registered on Kong.
        """
        list_microservices = self.get_available_microservices()
        return microservice in list_microservices

    def get_available_microservices(self) -> List[str]:
        """Get a list of all available microservices.

        Returns:
            List[str]:
                A list containing the names of all registered
                microservices.
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
        """Ping the database connection and restore the session if needed.

        Returns:
            Session:
                A valid SQLAlchemy session.
        """
        return get_session(db=self.db)

    @classmethod
    def pumpwood_pk_get(cls, pk: Union[str, int]) -> object:
        """Get model_class object using pumpwood pk.

        .. warning::
            This method is deprecated. Use `model_class.default_query_get`
            instead.

        Pumpwood primary keys may be integers or base64 strings encoding
        a dictionary for composite primary keys.

        Args:
            pk (str | int):
                The Pumpwood primary key.

        Returns:
            object:
                A SQLAlchemy object corresponding to the primary key.
        """
        msg = (
            "Use of 'pumpwood_pk_get' method is deprected, use "
            "model_class.default_query_get instead.")
        logger.warning(msg)

        model_object = cls.model_class.default_query_get(pk=pk)
        return model_object

    @classmethod
    def create_route_object(cls, service_object: dict) -> dict:
        """Build and register a Route object at the authorization service.

        Creates a KongRoute entry which registers the view's URL patterns
        in Kong, enabling access through the microservice gateway.

        Args:
            service_object (dict):
                A serialized KongService object which will host the
                new route.

        Returns:
            dict:
                The serialized KongRoute object returned by the server.

        Raises:
            PumpWoodOtherException:
                If registration fails at the microservice level.
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
                return cls.microservice.save(route_object)
            except Exception as e:
                msg = "Error when registering model [{model}]:\n{msg}".format(
                     model=model_class_name.lower(), msg=str(e))
                raise exceptions.PumpWoodOtherException(msg)

    @staticmethod
    def _allowed_extension(filename: str, allowed_extensions: list) -> list:
        """Validate if a file extension is permitted.

        Args:
            filename (str):
                The full name of the uploaded file.
            allowed_extensions (list):
                List of strings representing permitted file extensions,
                or ['*'] for any.

        Returns:
            list:
                A list containing an error message string if the
                extension is not allowed. An empty list if valid.
        """
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

    @staticmethod
    def _get_request_payload(request) -> dict:
        """Get request payload treating both the JSON and form-data.

        Args:
            request (flask.Request):
                Flask standard request object.

        Returns:
            dict:
                A dictionary containing the parsed payload data. Returns
                None if the HTTP method is not POST or PUT.

        Raises:
            PumpWoodWrongParameters:
                If information on __json__ can not be loaded.
        """
        if request.method.lower() not in ('post', 'put'):
            return None

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

            # It is expected that all fields will be sent as a serialized
            # value at __json__. In some simple forms the data may be passed
            # using form values, developer criteria.
            data.update(json_data)
        return data

    def dispatch_request(self, end_point: str, first_arg: str = None,
                         second_arg: str = None) -> Response:
        """Dispatch the request according to the endpoint and arguments.

        Handles authentication checks, payload extraction, and routes
        the execution to the specific CRUD, action, or option method.

        Args:
            end_point (str):
                The endpoint identifier (e.g., 'list', 'retrieve').
            first_arg (str):
                The first argument from the URL (often the PK).
            second_arg (str):
                The second argument (often an action name).

        Returns:
            Response:
                A standard Flask Response or jsonify output.

        Raises:
            PumpWoodException:
                If the endpoint or method combination is not implemented.
        """
        # Ping the database and rollback session if necessary
        self.get_session()

        # Force model to be init and avoid 'DeclarativeAttributeIntercept'
        AuthFactory.check_authorization(
            request_method=request.method.lower(),
            path=request.path, end_point=end_point,
            first_arg=first_arg, second_arg=second_arg,
            payload_text=request.get_data()[:300])

        # Extract data for post requests
        data = self._get_request_payload(request=request)

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
                    use_cache = json.loads(
                        request.args.get('use_cache', 'false'))
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
                    default_fields=default_fields,
                    use_cache=use_cache))

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
            upsert = json.loads(
                request.args.get('upsert', 'false'))
            save_data = self.save(
                data=data, fields=fields,
                foreign_key_fields=foreign_key_fields,
                related_fields=related_fields,
                default_fields=default_fields,
                upsert=upsert)
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

        raise PumpWoodFlaskViewEndPointFoundError(
            message=(
                'End-point {end_point} for method {method} not implemented'),
            payload={
                'end_point': end_point,
                'method': request.method})

    @classmethod
    def as_view(cls, *class_args, **class_kwargs):
        """Set view name as model_class name."""
        return super(PumpWoodFlaskView, cls).as_view(
            name=cls.model_class.__name__, *class_args, **class_kwargs)

    def list(self, filter_dict: None | dict = None,
             exclude_dict: dict = None, order_by: list = None,
             fields: list = None, limit: int = None,
             default_fields: bool = False,
             foreign_key_fields: bool = False, **kwargs) -> list:
        """Return a paginated list of serialized objects.

        Args:
            filter_dict (dict):
                Dictionary for filtering operations.
            exclude_dict (dict):
                Dictionary for exclusion operations.
            order_by (list):
                List of fields to order the results by.
            fields (list):
                Specific fields to be returned in the response.
            limit (int):
                Maximum number of objects to return.
            default_fields (bool):
                If True, returns only the default fields defined for
                the list view.
            foreign_key_fields (bool):
                If True, expands foreign key fields into full objects.
            **kwargs:
                For compatibility and extensibility.

        Returns:
            list:
                A list of serialized dictionaries representing the
                matching objects.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by

        list_paginate_limit = limit or self.list_paginate_limit
        query_result = self.model_class.default_query_list(
            filter_dict=filter_dict, exclude_dict=exclude_dict,
            order_by=order_by, limit=list_paginate_limit)

        list_serializer = self.serializer(
            many=True, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=False)
        return list_serializer.dump(query_result)

    def list_without_pag(self, filter_dict: None | dict = None,
                         exclude_dict: dict = None, order_by: list = None,
                         fields: list = None, default_fields: bool = False,
                         foreign_key_fields: bool = False, **kwargs) -> list:
        """Return all matching objects without pagination.

        Args:
            filter_dict (dict):
                Dictionary for filtering operations.
            exclude_dict (dict):
                Dictionary for exclusion operations.
            order_by (list):
                List of fields to order the results by.
            fields (list):
                Specific fields to be returned.
            default_fields (bool):
                If True, returns the default fields for the list view.
            foreign_key_fields (bool):
                If True, expands foreign keys.
            **kwargs:
                For compatibility and extensibility.

        Returns:
            list:
                A list of all matching serialized objects.
        """
        # Set list and dicts in the fuction to no bug with pointers
        filter_dict = {} if filter_dict is None else filter_dict
        exclude_dict = {} if exclude_dict is None else exclude_dict
        order_by = [] if order_by is None else order_by

        query_result = self.model_class.default_query_list(
            filter_dict=filter_dict, exclude_dict=exclude_dict,
            order_by=order_by)

        list_serializer = self.serializer(
            many=True, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=False)
        return list_serializer.dump(query_result)

    def retrieve(self, pk: Any, fields: list = None,
                 foreign_key_fields: bool = False,
                 related_fields: bool = False,
                 default_fields: bool = False,
                 use_cache: bool = False) -> dict:
        """Retrieve a single object by its primary key.

        Args:
            pk (int | str | dict):
                The primary key identifier (integer, base64 dict, or dict).
            fields (list):
                Specific fields to be returned in the response.
            foreign_key_fields (bool):
                If True, expands foreign key fields into full objects.
            related_fields (bool):
                If True, expands related (M2M) fields.
            default_fields (bool):
                If True, returns the default fields for the view.
            use_cache (bool):
                If True, allows reading from the request-scoped cache.

        Returns:
            dict:
                The serialized dictionary representing the object.
        """
        model_object = self.model_class.default_query_get(
            pk=pk, use_cache=use_cache)
        retrieve_serializer = self.serializer(
            many=False, fields=fields, default_fields=default_fields,
            foreign_key_fields=foreign_key_fields,
            related_fields=related_fields)
        return retrieve_serializer.dump(model_object)

    def retrieve_file(self, pk: int | str, file_field: str) -> dict:
        """Read a file associated with a model field as a single byte block.

        Args:
            pk (int | str):
                Primary key of the object containing the file field.
            file_field (str):
                The name of the field that stores the file path.

        Returns:
            dict:
                A dictionary containing 'data' (bytes), 'content_type',
                and 'file_name'.

        Raises:
            PumpWoodForbidden:
                If storage is not configured or field is not allowed.
            PumpWoodObjectDoesNotExist:
                If the object or the file itself is missing in storage.
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
        """Read a file associated with a model field using a stream iterator.

        Args:
            pk (int | str):
                Primary key of the object containing the file field.
            file_field (str):
                The name of the field that stores the file path.

        Returns:
            iterator:
                A stream of bytes from the storage object.
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
        obj = self.model_class.default_query_get(pk=pk)
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

    def object_template(self) -> dict:
        """Return an empty serialized object to act as a template.

        Useful for front-ends to instantiate a form for creating a
        new object with the correct structure.

        Returns:
            dict:
                A serialized dictionary of an empty model instance.
        """
        empty_object = self.model_class()
        retrieve_serializer = self.serializer(many=False)
        return retrieve_serializer.dump(empty_object)

    def delete(self, pk: int | str, force_delete: bool = False) -> dict:
        """Delete an object by its primary key.

        Supports soft-deletion if the model has a 'deleted' column.
        If 'force_delete' is False and the column exists, it sets
        'deleted' to True instead of removing the row.

        Args:
            pk (int | str):
                The primary key of the object to be deleted.
            force_delete (bool):
                If True, bypasses soft-delete logic and removes the
                record from the database.

        Returns:
            dict:
                The serialized representation of the deleted object.
        """
        session = self.get_session()
        model_object = self.model_class.default_query_get(pk=pk)
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
        is_to_broadcast = self.broadcast and pumpwood_etl_ok
        if self.microservice is not None and is_to_broadcast:
            # Process ETLTrigger for the model class
            self.microservice.login()
            self.microservice.execute_action(
                "ETLTrigger", action="process_triggers", parameters={
                    "model_class": self.model_class.__name__.lower(),
                    "type": "delete",
                    "pk": object_dump["pk"],
                    "action_name": None})
        return object_dump

    def delete_many(self, filter_dict: dict = None,
                    exclude_dict: dict = None) -> bool:
        """Delete multiple objects matching the filter criteria.

        Args:
            filter_dict (dict):
                Filters identifying objects to be deleted.
            exclude_dict (dict):
                Filters identifying objects to be spared.

        Returns:
            bool:
                Always returns True if the operation succeeds.
        """
        session = self.get_session()
        try:
            # User will only be abble to delete objects associated with his
            # row permission
            base_query = self._add_default_filter()
            query_result = SqlalchemyQueryMisc\
                .sqlalchemy_kward_query(
                    object_model=self.model_class,
                    base_query=base_query,
                    filter_dict=filter_dict,
                    exclude_dict=exclude_dict)
            query_result.delete(synchronize_session=False)
            session.commit()

        except Exception as e:
            session.rollback()
            raise e
        return True

    def save(self, data: dict, file_paths: dict = None,
             foreign_key_fields: bool = False, related_fields: bool = False,
             default_fields: bool = False, fields: list = None,
             upsert: bool = False) -> dict:
        """Update an existing object or save a new one.

        Handles complex serialization, file uploads, and optional upsert
        behavior.

        Args:
            data (dict):
                The object data payload.
            file_paths (dict):
                Internal parameter for direct file path assignment.
            foreign_key_fields (bool):
                If True, includes expanded FKs in the response.
            related_fields (bool):
                If True, includes expanded related fields.
            default_fields (bool):
                If True, uses default list fields for serialization.
            fields (list):
                Limits the returned object to specific fields.
            upsert (bool):
                If True, creates a new record if the PK is not found.

        Returns:
            dict:
                The serialized representation of the saved object.

        Raises:
            PumpWoodObjectSavingException:
                If validation fails or file uploads encounter errors.
        """
        # Fill default values if not provided
        file_paths = {} if file_paths is None else file_paths

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
            # If it is an upsert operation and the object is not found,
            # it will not raise not found error and return an empty
            # object
            model_object = self.model_class.default_query_get(
                pk=pk, raise_error=not upsert)

        # Set if the object is new, this will be returned at the
        # object results to differenciate the upsert operation
        is_new_object = model_object is None

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
        is_to_broadcast = self.broadcast and pumpwood_etl_ok
        if self.microservice is not None and is_to_broadcast:
            # Process ETL Trigger for the model class
            self.microservice.login()
            if not is_new_object:
                self.microservice.execute_action(
                    "ETLTrigger", action="process_triggers", parameters={
                        "model_class": self.model_class.__name__.lower(),
                        "type": "create",
                        "pk": None,
                        "action_name": None,
                        "extra_info": result})
            else:
                self.microservice.execute_action(
                    "ETLTrigger", action="process_triggers", parameters={
                        "model_class": self.model_class.__name__.lower(),
                        "type": "update",
                        "pk": result["pk"],
                        "action_name": None,
                        "extra_info": result})

        # 
        result['__is_new_object__'] = is_new_object
        return result

    def save_file_streaming(self, pk: int | str, file_field: str,
                            file_name: str = None, **kwargs) -> dict:
        """Save a file to an object using streamed data.

        Args:
            pk (int | str):
                The primary key of the object to be updated.
            file_field (str):
                The field name in the model that stores the file path.
            **kwargs (str):
                Other arguments used on the save end-point.

        Returns:
            dict:
                The response from the storage provider after upload.
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

    def get_actions(self) -> dict:
        """Retrieve all methods decorated as actions.

        Scans the model class for methods and functions that have the
        'is_action' attribute set to True.

        Returns:
            dict:
                A dictionary mapping the action name to its respective
                function reference.
        """
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

    def list_actions_with_objects(self, objects: dict) -> list:
        """List model exposed actions according to selected objects.

        Args:
            objects (dict):
                A payload or dictionary representing selected objects
                from the front-end.

        Returns:
            list:
                A list of strings containing descriptions of the
                applicable actions.
        """
        actions = self.get_actions()
        action_descriptions = [
            action.action_object.description
            for name, action in actions.items()]
        return action_descriptions

    def execute_action(self, action_name: str, pk: Any = None,
                       parameters: dict = None) -> dict:
        """Execute a decorated action on the model or instance.

        Actions can be static or instance-based. This method handles
        object retrieval, parameter validation, and execution.

        Args:
            action_name (str):
                The name of the action to execute.
            pk (Any):
                The primary key (required if action is instance-based).
            parameters (dict):
                A dictionary of parameters passed to the action function.

        Returns:
            dict:
                A dictionary containing 'result', 'action', 'parameters',
                and the serialized 'object' state.
        """
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

            model_object = self.model_class.default_query_get(pk=pk)

            # Retrieve function associated with action to inject the object as
            # self parameter
            action_fun = getattr(model_object, action_name)

            # Create a serializer to serialize the object to return the value
            # at the action call
            temp_serializer = self.serializer(many=False, default_fields=True)
            object_dict = temp_serializer.dump(model_object)

        loaded_parameters = LoadActionParameters.load(
            func=action_fun, parameters=parameters)
        result = action_fun(**loaded_parameters)

        available_microservices = self.get_available_microservices()
        pumpwood_etl_ok = 'pumpwood-etl-app' in available_microservices
        is_to_broadcast = (self.broadcast and pumpwood_etl_ok)
        if self.microservice is not None and is_to_broadcast:
            self.microservice.login()
            self.microservice.execute_action(
                "ETLTrigger", action="process_triggers", parameters={
                    "model_class": self.model_class.__name__.lower(),
                    "type": "action", "pk": pk, "action_name": action_name})

        return {
            'result': result, 'action': action_name,
            'parameters': parameters, 'object': object_dict}

    def aggregate(self, group_by: List[str], agg: dict,
                  filter_dict: dict = None, exclude_dict: dict = None,
                  order_by: List[str] = None, limit: int = None,
                  format: str = 'list', **kwargs) -> Union[dict, list]:
        """Aggregate database information using group_by and functions.

        Args:
            group_by (List[str]):
                Columns used in the GROUP BY clause.
            agg (dict):
                Aggregation dictionary mapping return keys to fields and
                functions (e.g., {'total': {'field': 'amount', 'func':
                'sum'}}).
            filter_dict (dict):
                Filters to apply before aggregation.
            exclude_dict (dict):
                Exclusions to apply before aggregation.
            order_by (List[str]):
                Ordering for the aggregated results.
            limit (int):
                Maximum number of results to return.
            format (str):
                Pandas dictionary format (e.g., 'list', 'records').
            **kwargs:
                Extra arguments.

        Returns:
            Union[dict, list]:
                The aggregated data in the requested format.
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

        base_query = self._add_default_filter()
        subquery_result = SqlalchemyQueryMisc\
            .sqlalchemy_kward_query(
                object_model=self.model_class,
                base_query=base_query,
                filter_dict=filter_dict,
                exclude_dict=exclude_dict)

        pd_results = pd.DataFrame(
            SqlalchemyQueryMisc.aggregate(
                session=session, object_model=self.model_class,
                query=subquery_result, group_by=group_by,
                agg=agg, order_by=order_by).limit(limit).all())
        return pd_results.to_dict(format)

    @classmethod
    def cls_fields_options(cls,
                           user_type: Literal['api', 'gui'] = 'api') -> dict:
        """Return description of the model fields."""
        # Get information from cache if avaiable
        return_data = AuxFillOptions.run(
            model_class=cls.model_class,
            serializer=cls.serializer,
            view_file_fields=cls.file_fields,
            user_type=user_type)
        return return_data

    def search_options(self) -> dict:
        """Retrieve search options for list pages.

        .. warning::
            This method is deprecated.

        Returns:
            dict:
                A dictionary describing the model fields.
        """
        return self.cls_fields_options()

    def fill_options(self, partial_data: dict, field: str = None) -> dict:
        """Retrieve fill options for retrieve and save pages.

        .. warning::
            This method is deprecated.

        Args:
            partial_data (dict):
                The current state of the data in the front-end.
            field (str):
                A specific field to focus on. Defaults to None.

        Returns:
            dict:
                A dictionary describing the model fields.
        """
        return self.cls_fields_options()

    def list_view_options(self) -> dict:
        """Return information required to render list views on front-ends.

        Returns:
            dict:
                A dictionary containing:
                - default_list_fields (List[str]): Fields to render.
                - field_descriptions (dict): Column metadata and filters.
        """
        list_fields = self.get_list_fields()
        fields_options = self.cls_fields_options()
        return {
            "default_list_fields": list_fields,
            "field_descriptions": fields_options}

    def retrieve_view_options(self) -> dict:
        """Return information required to render retrieve views on front-ends.

        Uses the `gui_retrieve_fieldset` attribute to define layout
        tabs and field groupings.

        Returns:
            dict:
                A dictionary containing:
                - verbose_field (str): Display name for the object.
                - fieldset (dict): Tab definitions and field groupings.
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
        """Return fill options with validation for retrieve/save pages.

        Args:
            partial_data (dict):
                Partial object data to be validated.
            user_type (str):
                Interface type ('api' or 'gui'). 'gui' enforces
                read-only constraints on specific fields.
            field (str):
                Optional specific field to validate.

        Returns:
            dict:
                A dictionary containing field descriptions and read-only
                constraints.
        """
        fill_options = self.cls_fields_options(user_type=user_type)
        serializer_obj = self.serializer()
        gui_readonly = serializer_obj.get_gui_readonly()
        return {
            "field_descriptions": fill_options,
            "gui_readonly": gui_readonly
        }
