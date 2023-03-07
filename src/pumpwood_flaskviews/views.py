"""Functions and classes to help build PumpWood End-Points."""
import os
import io
import copy
import pandas as pd
import textwrap
import inspect
import datetime
import psycopg2
import sqlalchemy
import simplejson as json
from typing import Any, Union
from sqlalchemy import inspect as sqlalchemy_inspect
from flask.views import View
from flask import request, json, Response
from flask import jsonify, send_file
from werkzeug.utils import secure_filename
from sqlalchemy import inspect as alchemy_inspect
from sqlalchemy_utils.types.choice import ChoiceType
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql.schema import Sequence, UniqueConstraint
from sqlalchemy.sql.expression import False_ as sql_false
from sqlalchemy.sql.expression import True_ as sql_true
from geoalchemy2.types import Geometry
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_miscellaneous.query import SqlalchemyQueryMisc
from .auth import AuthFactory
from .action import load_action_parameters


class PumpWoodFlaskView(View):
    """
    PumpWoodFlaskView base view for pumpwood like models.

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

    @classmethod
    def pumpwood_pk_get(cls, pk: Union[str, int]) -> object:
        """
        Get model_class object using pumpwood pk.

        Pumpwood pk may be integers and base64 strings coding a dictionary
        with composite primary keys. This function abstract SQLAlchemy
        query.get to treat both possibilities.

        Args:
            pk [str, int]: Pumpwood primary key.
        Return [object]:
            Returns a SQLAlchemy object with corresponding primary key.
        """
        converted_pk = CompositePkBase64Converter.load(pk)
        model_object = cls.model_class.query.get(converted_pk)
        return model_object

    @classmethod
    def create_route_object(cls, service_object: dict) -> dict:
        """
        Build Route object from view information.

        Creates a route object on admin microservice, which will register a
        route in Kong using service created by service_object.

        Args:
            service_object [dict:KongService]: A serialized KongService object
                on which will be registred the new route.
        Return [dict: KongRoute]:
            Returns a serialized object of KongRoute.
        """
        if service_object is not None:
            print("## Registering route on auth")
            cls.microservice.login()
            model_class_name = cls.model_class.__name__
            suffix = os.getenv('ENDPOINT_SUFFIX', '')
            model_class_name = suffix + model_class_name

            route_url = '/rest/%s/' % model_class_name.lower()
            route_name = model_class_name.lower()

            search_options = cls.cls_search_options()
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
                    "foreign_keys": cls.foreign_keys,
                    "list_fields": cls.list_fields,
                    "file_fields": cls.file_fields,
                    "search_options": search_options
                }}
            try:
                cls.microservice.save(route_object)
            except Exception as e:
                msg = "Error when registering model [{model}]:\n{msg}".format(
                     model=model_class_name.lower(), msg=str(e))
                print(msg)
                raise e

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
        AuthFactory.check_authorization()

        data = None
        if request.method.lower() in ('post', 'put'):
            if request.mimetype == 'application/json':
                data = request.get_json()
            else:
                data = request.form.to_dict()
                for k in data.keys():
                    data[k] = json.loads(data[k])

        # List end-points
        if end_point == 'list' and request.method.lower() == 'post':
            endpoint_dict = data or {}
            return jsonify(self.list(**endpoint_dict))

        if end_point == 'list-without-pag' and \
           request.method.lower() == 'post':
            endpoint_dict = data or {}
            return jsonify(self.list_without_pag(**endpoint_dict))

        # Retrieve with list serializer
        if end_point == 'list-one' and request.method.lower() == 'get':
            if first_arg is None:
                raise exceptions.ObjectDoesNotExist('url pk is None')
            return jsonify(self.list_one(pk=first_arg))

        # Retrive end-points
        if end_point == 'retrieve':
            if first_arg is None:
                return jsonify(self.object_template())

            if request.method.lower() == 'get':
                return jsonify(self.retrieve(pk=first_arg))

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
                    attachment_filename=file_data["file_name"],
                    mimetype=file_data["content_type"])

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
            return jsonify(self.save(data=data))

        if end_point == "save-file-streaming" and \
                request.method.lower() in ('post', 'put'):
            if first_arg is None:
                raise exceptions.PumpWoodException(
                    "Save file stream endpoint have a pk")
            
            # Get url parameters for the end-point
            file_field = request.args.get('file_field')
            if file_field is None:
                raise exceptions.PumpWoodForbidden(
                    "file_field not set as url parameter")
            file_name = request.args.get('file_name')
            return jsonify(self.save_file_streaming(
                pk=first_arg, file_field=file_field, file_name=file_name))

        if end_point == "remove-file-field" and \
                request.method.lower() in ('delete'):
            file_field = request.args.get('file_field')
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
                return jsonify(self.delete(pk=first_arg))

            if request.method.lower() == 'post':
                endpoint_dict = data or {}
                return jsonify(self.delete_many(**endpoint_dict))

        # actions end-points
        if end_point == 'actions':
            if request.method.lower() == 'get':
                return jsonify(self.list_actions())

            elif request.method.lower() == 'post':
                if first_arg is None:
                    return jsonify(
                        self.list_actions_with_objects(objects=data))
                else:
                    return jsonify(self.execute_action(
                        action_name=first_arg, pk=second_arg, parameters=data))

        # options end-points
        if end_point == 'options':
            if request.method.lower() == 'get':
                return jsonify(self.search_options())

            if request.method.lower() == 'post':
                return jsonify(self.fill_options(partial_data=data,
                                                 field=first_arg))

        raise exceptions.PumpWoodException(
            'End-point %s for method %s not implemented' % (
                end_point, request.method))

    @classmethod
    def as_view(cls, *class_args, **class_kwargs):
        """Set view name as model_class name."""
        return super(PumpWoodFlaskView, cls).as_view(
            name=cls.model_class.__name__, *class_args, **class_kwargs)

    def list(self, filter_dict: dict = {}, exclude_dict: dict = {},
             order_by: list = [], fields: list = None,
             limit: int = None, default_fields: bool = False,
             **kwargs) -> list:
        """
        Return query result pagination.

        Args:
            No args.

        Kwargs:
            filter_dict [dict]: Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict [dict]: Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by [list]: Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            fields [list]: Fields to be returned.
            limit [int]: Number of objects to be returned.
            default_fields [bool]: Return the fields specified at
                self.list_fields.
        Return [list]:
            Return a list of serialized objects using self.serializer and
            filtered by args.
        """
        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict
        to_function_dict['order_by'] = order_by

        list_paginate_limit = limit or self.list_paginate_limit
        query_result = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            **to_function_dict).limit(list_paginate_limit).all()

        # If field is set always return the requested fields.
        if fields is not None:
            temp_fields = fields
        # default_fields is True, return the ones specified by self.list_fields
        elif default_fields:
            temp_fields = self.list_fields
        # If default_fields not set return all object fields.
        else:
            temp_fields = None

        temp_serializer = self.serializer(many=True, only=temp_fields)
        return temp_serializer.dump(query_result, many=True).data

    def list_without_pag(self, filter_dict: dict = {}, exclude_dict: dict = {},
                         order_by: list = [], fields: list = None,
                         default_fields: bool = False, **kwargs) -> list:
        """
        Return query without pagination.

        Args:
            No args.
        Kargs:
            filter_dict [dict]: Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            exclude_dict [dict]: Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            order_by [list]: Dictionary to be used in filter operations.
                See pumpwood_miscellaneous.SqlalchemyQueryMisc documentation.
            fields [list]: Fields to be returned.
            default_fields [bool]: Return the fields specified at
                self.list_fields.
        Return [list]:
            Return a list of serialized objects using self.serializer and
            filtered by args without pagination all values.
        """
        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict
        to_function_dict['order_by'] = order_by
        query_result = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            **to_function_dict).all()

        # If field is set always return the requested fields.
        if fields is not None:
            temp_fields = fields
        # default_fields is True, return the ones specified by self.list_fields
        elif default_fields:
            temp_fields = self.list_fields
        # If default_fields not set return all object fields.
        else:
            temp_fields = None

        temp_serializer = self.serializer(many=True, only=temp_fields)
        return temp_serializer.dump(query_result, many=True).data

    def list_one(self, pk: int, fields: list = None) -> dict:
        """
        Use List Serializer to return object with pk.

        Args:
            pk [int]: Primary key of the object to be returned.
        Kwargs:
            fields [list(str)]: List of the fields that should be returned,
                if None is passed self.list_fields will be used.

        Return [dict]:
            A dictonary with the serialized values of the object using list
            fields to restric the returned values. It is possible to specify
            which values should be returned using fields.
        """
        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

        model_object = self.pumpwood_pk_get(pk=pk)
        if pk is not None and model_object is None:
            temp_model_class = self.model_class.__mapper__.class_.__name__
            try:
                pk = int(pk)
            except Exception:
                pass

            message = "Requested object {model_class}[{pk}] not found.".format(
                model_class=temp_model_class, pk=pk)
            raise exceptions.PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": temp_model_class,
                    "pk": pk})

        temp_fields = fields or self.list_fields
        if temp_fields is not None:
            temp_fields = list({"pk", "model_class"}.union(set(temp_fields)))

        temp_serializer = self.serializer(many=False, only=temp_fields)
        return temp_serializer.dump(model_object).data

    def retrieve(self, pk: Any) -> dict:
        """
        Retrieve object with pk.

        Args:
            pk [int]: Primary key of the object to be returned.
        Kwargs:
            composite_pk [dict]: Add extra data to combine with
                main primary, this is necessary mainly on variable
                tables that are indexed using many keys.
        Return [dict]:
            A dictionary with the serialized values of the object.
        """
        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

        model_object = self.pumpwood_pk_get(pk=pk)
        retrieve_serializer = self.serializer(many=False)
        if pk is not None and model_object is None:
            temp_model_class = self.model_class.__mapper__.class_.__name__
            try:
                pk = int(pk)
            except Exception:
                pass

            message = "Requested object {model_class}[{pk}] not found.".format(
                model_class=temp_model_class,
                pk=pk)
            raise exceptions.PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": temp_model_class,
                    "pk": pk})

        return retrieve_serializer.dump(model_object).data

    def retrieve_file(self, pk: int, file_field: str):
        """
        Read file without stream.

        Args:
            pk (int): Pk of the object to save file field.
            file_field(str): File field to receive stream file.

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
            temp_model_class = self.model_class.__mapper__.class_.__name__
            raise exceptions.PumpWoodObjectDoesNotExist(
                message=msg, payload={
                    "model_class": temp_model_class,
                    "pk": object_data["pk"], "file_path": file_path})

        file_data = self.storage_object.read_file(file_path)
        file_name = os.path.basename(file_path)
        file_data["file_name"] = file_name
        return file_data

    def retrieve_file_streaming(self, pk: int, file_field: str):
        """
        Read file using stream.

        Args:
            pk (int): Pk of the object to save file field.
            file_field(str): File field to receive stream file.

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

    def remove_file_field(self, pk: int, file_field: str) -> bool:
        """
        Remove file field.

        Args:
            pk (int): pk of the object.
            file_field (str): name of the file field.
        Kwargs:
            No kwargs for this function.
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

        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

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
        return retrieve_serializer.dump(empty_object).data

    def delete(self, pk):
        """Delete object."""
        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

        model_object = self.pumpwood_pk_get(pk=pk)
        if pk is not None and model_object is None:
            message = "Requested object {model_class}[{pk}] not found.".format(
                model_class=self.model_class.__mapper__.class_.__name__, pk=pk)
            try:
                pk = int(pk)
            except Exception:
                pass

            raise exceptions.PumpWoodObjectDoesNotExist(
                message=message, payload={
                    "model_class": self.model_class.__mapper__.class_.__name__,
                    "pk": pk})

        temp_serializer = self.serializer(many=False)
        object_dump = temp_serializer.dump(model_object, many=False).data
        try:
            session.delete(model_object)
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            raise exceptions.PumpWoodIntegrityError(message=str(e))
        except psycopg2.errors.IntegrityError as e:
            session.rollback()
            raise exceptions.PumpWoodIntegrityError(message=str(e))
        except Exception as e:
            session.rollback()
            raise exceptions.PumpWoodObjectDeleteException(message=str(e))

        if self.microservice is not None and self.trigger:
            # Process ETLTrigger for the model class
            self.microservice.login()
            self.microservice.execute_action(
                "ETLTrigger", action="process_triggers", parameters={
                    "model_class": self.model_class.__name__.lower(),
                    "type": "delete",
                    "pk": object_dump["pk"],
                    "action_name": None})
        return object_dump

    def delete_many(self, filter_dict={}, exclude_dict={}):
        """Delete object."""
        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()
        ###############################################

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
            raise exceptions.PumpWoodObjectDeleteException(message=str(e))
        return True

    def save(self, data, file_paths: dict = {}):
        """Update object or save new object."""
        retrieve_serializer = self.serializer(many=False)
        retrieve_serializer.context['authorization_token'] = \
            request.headers.get('Authorization', None)

        ###############################################
        # Check if database connection of session is Ok
        session = self.db.session
        try:
            session.execute("SELECT 1;")
        except Exception:
            self.db.engine.dispose()
            session.rollback()

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
        if pk is not None:
            # Query object to update
            model_object = self.pumpwood_pk_get(pk=pk)
            if model_object is None:
                temp_model_class = self.model_class.__mapper__.class_.__name__

                # Convert to integer for error payload
                try:
                    pk = int(pk)
                except Exception:
                    pass
                message = (
                    "Requested object {model_class}[{pk}] not found.").format(
                    model_class=temp_model_class, pk=pk)
                raise exceptions.PumpWoodObjectDoesNotExist(
                    message=message, payload={
                        "model_class": temp_model_class, "pk": pk})
            to_save_obj = retrieve_serializer.load(
                data, instance=model_object, session=session)
        else:
            to_save_obj = retrieve_serializer.load(data, session=session)

        # True if errors were found at the validation of the fields
        with_save_error = to_save_obj.errors != {}
        if not with_save_error:
            try:
                # Flush object to receive it's id to create file name,
                # but does not commit so if there is file errors it won't
                # persist on database.
                session.add(to_save_obj.data)
                session.flush()
            except sqlalchemy.exc.IntegrityError as e:
                session.rollback()
                raise exceptions.PumpWoodIntegrityError(message=str(e))
            except psycopg2.errors.IntegrityError as e:
                session.rollback()
                raise exceptions.PumpWoodIntegrityError(message=str(e))
            except Exception as e:
                session.rollback()
                raise exceptions.PumpWoodException(message=str(e))

        # Set file names with file_paths dict which is not exposed to API
        # this is only used by save_file_streaming to set file name
        for key, path in file_paths.items():
            setattr(to_save_obj.data, key, path)

        # True if files were added to the object
        with_files = False
        file_save_time = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%Hh%Mm%Ss")
        if not with_save_error:
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
                        with_save_error = True

                    # Check if only one files was uploaded for the file field
                    # more than one file is not implemented
                    if len(files_list) != 1:
                        msg = "More than one file passed."
                        field_errors.append(msg)
                        with_save_error = True
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
                            with_save_error = True
                        else:
                            full_filename = "{}___{}___{}".format(
                                to_save_obj.data.id, file_save_time,
                                filename)

                    if len(field_errors) != 0:
                        to_save_obj.errors[field] = field_errors

                    # Check if object does not have errors so far, if have
                    # does not apply file changes
                    if not with_save_error:
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
                            to_save_obj.data, field,
                            storage_filepath)

                        # Get hash if there is a {field}_hash on
                        # object attributes
                        field_hash = "{}_hash".format(field)
                        if hasattr(to_save_obj.data, field_hash):
                            file_hash = \
                                self.storage_object.get_file_hash(
                                    file_path=storage_filepath)
                            setattr(
                                to_save_obj.data, field_hash,
                                file_hash)

                        # Mark that a file has been added to object and save
                        # the path latter.
                        with_files = True

        if with_save_error:
            message = "error when saving object: " \
                if pk is None else "error when updating object: "
            payload = to_save_obj.errors
            message_to_append = []
            for key, value in to_save_obj.errors.items():
                message_to_append.append(key + ", " + str(value))
            message = message + "; ".join(message_to_append)
            session.rollback()
            raise exceptions.PumpWoodObjectSavingException(
                message=message, payload=payload)

        # If with files, update object on database to have uploaded file
        # paths        
        if with_files:
            session.add(to_save_obj.data)

        # Commit file changes to database and persist object with file
        # information if present.
        try:
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            raise exceptions.PumpWoodIntegrityError(message=str(e))
        except psycopg2.errors.IntegrityError as e:
            session.rollback()
            raise exceptions.PumpWoodIntegrityError(message=str(e))
        except Exception as e:
            session.rollback()
            raise exceptions.PumpWoodException(message=str(e))

        result = retrieve_serializer.dump(to_save_obj.data).data

        ###################################
        # Pumpwood ETLTrigger integration #
        if self.microservice is not None and self.trigger:
            # Process ETLTrigger for the model class
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

    def save_file_streaming(self, pk: int, file_field: str,
                            file_name: str = None):
        """
        Save file to object.

        Args:
            pk (int): Pk of the object to be updated.
            file_field (str): Name of the file field in the object.
        Kwargs:
            No kwargs for this function.
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
        file_name = "{pk}__{filename}".format(pk=pk, filename=file_name)

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
        action_descriptions = [
            action.action_object.to_dict()
            for name, action in actions.items()]
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
        actions = self.get_actions()
        rest_action_names = list(actions.keys())

        if action_name not in rest_action_names:
            message = ("There is no method {action} in rest actions "
                       "for {class_name}").format(
                action=action_name,
                class_name=self.model_class.__mapper__.class_.__name__)
            raise exceptions.PumpWoodException(
                message=message, payload={"action_name": action_name})

        action = getattr(self.model_class, action_name)
        action_object = action.action_object.to_dict()

        object_dict = None
        if pk is None:
            if not action_object["is_static_function"]:
                raise exceptions.PumpWoodActionArgsException(
                    "Function is not static and pk is Null")
        else:
            if action_object["is_static_function"]:
                raise exceptions.PumpWoodActionArgsException(
                    "Function is static and pk is not Null")

            model_object = self.pumpwood_pk_get(pk=pk)
            if model_object is None:
                message_template = "Requested object {model_class}[{pk}] " + \
                    "not found."
                temp_model_class = self.model_class.__mapper__.class_.__name__

                try:
                    pk = int(pk)
                except Exception:
                    pass

                message = message_template.format(
                    model_class=temp_model_class, pk=pk)
                raise exceptions.PumpWoodObjectDoesNotExist(
                    message=message, payload={
                        "model_class": temp_model_class,
                        "pk": pk})
            action = getattr(model_object, action_name)

            temp_serializer = self.serializer(
                many=False, only=self.list_fields)
            object_dict = temp_serializer.dump(model_object, many=False).data

        loaded_parameters = load_action_parameters(action, parameters)
        result = action(**loaded_parameters)

        if self.microservice is not None and self.trigger:
            self.microservice.login()
            self.microservice.execute_action(
                "ETLTrigger", action="process_triggers", parameters={
                    "model_class": self.model_class.__name__.lower(),
                    "type": "action", "pk": pk, "action_name": action_name})

        return {
            'result': result, 'action': action_name,
            'parameters': parameters, 'object': object_dict}

    @classmethod
    def cls_search_options(cls):
        mapper = alchemy_inspect(cls.model_class)
        dump_only_fields = getattr(cls.serializer.Meta, "dump_only", [])

        # Getting table/class map
        table_class_map = {}
        for clazz in cls.db.Model._decl_class_registry.values():
            try:
                table_class_map[clazz.__tablename__] = clazz.__name__
                table_names.append(clazz.__tablename__)
            except:
                pass

        dict_columns = {}
        for x in mapper.columns:
            column_inspect = alchemy_inspect(x)

            type_str = None
            if isinstance(x.type, Geometry):
                type_str = "geometry"
            else:
                type_str = x.type.python_type.__name__

            read_only = False
            if x.name in dump_only_fields:
                read_only = True

            column_info = {
                "primary_key": x.primary_key,
                "column": x.name,
                "doc_string": x.doc,
                "type": type_str,
                "nullable": x.nullable,
                "read_only": read_only,
                "default": None,
                "unique": x.unique}

            if isinstance(x.type, ChoiceType):
                column_info["type"] = "options"
                column_info["in"] = [
                    {"value": choice[0], "description": choice[1]}
                    for choice in x.type.choices]

            if column_info["column"] == "id":
                column_info["default"] = "#autoincrement#"
                column_info["doc_string"] = "autoincrement id"
                relationships = mapper.relationships.items()
                for rel in relationships:
                    relation_col = list(rel[1].local_columns)[0]
                    rel_table = relation_col.table.fullname
                    rel_class = table_class_map.get(rel_table)
                    if cls.relationships.get(rel_class) is None:
                        cls.relationships[rel_class] = relation_col.name
                column_info["relationships"] = cls.relationships

            foreign_keys = list(x.foreign_keys)
            micro_fk = cls.foreign_keys.get(x.name)
            if len(foreign_keys) != 0:
                # Try to fetch model class using table name
                fk = foreign_keys[0]
                fk_table = fk.column.table.fullname
                fk_class = table_class_map.get(fk_table)
                column_info["type"] = "foreign_key"
                column_info["model_class"] = fk_class
                cls.foreign_keys[x.name] = fk_class

            elif micro_fk is not None:
                column_info["type"] = "foreign_key"
                if isinstance(micro_fk, dict):
                    column_info["model_class"] = micro_fk["model_class"]
                elif isinstance(micro_fk, str):
                    column_info["model_class"] = micro_fk
                else:
                    msg = (
                        "foreign_key not correctly defined, check column"
                        "[{column}] from model [{model}]").format(
                            column=x.name,
                            model=cls.model_class.__name__)
                    raise Exception(msg)

            file_field = cls.file_fields.get(x.name)
            if file_field is not None:
                column_info["type"] = "file"
                column_info["permited_file_types"] = file_field

            if x.default is not None:
                arg = getattr(x.default, 'arg', None)
                if isinstance(arg, GenericFunction):
                    column_info["default"] = arg.description
                elif arg == dict:
                    column_info["default"] = {}
                elif inspect.isfunction(arg):
                    column_info["default"] = arg.__name__ + "()"
                elif isinstance(x.default, Sequence):
                    column_info["default"] = "#autoincrement#"
                elif isinstance(arg, sql_false):
                    column_info["default"] = False
                elif isinstance(arg, sql_true):
                    column_info["default"] = True
                else:
                    column_info["default"] = arg

            elif x.server_default is not None:
                arg = getattr(x.server_default, 'arg', None)
                if isinstance(arg, GenericFunction):
                    column_info["default"] = arg.description
                elif arg == dict:
                    column_info["default"] = {}
                elif inspect.isfunction(arg):
                    column_info["default"] = arg.__name__ + "()"
                elif isinstance(x.server_default, Sequence):
                    column_info["default"] = "#autoincrement#"
                elif isinstance(arg, sql_false):
                    column_info["default"] = False
                elif isinstance(arg, sql_true):
                    column_info["default"] = True
                else:
                    column_info["default"] = arg

            dict_columns[column_info["column"]] = column_info
        
        ############################################################
        # Stores primary keys as attribute to help other functions #
        if cls._primary_keys is None:
            cls._primary_keys = [
                key for key, item in dict_columns.items()
                if item["primary_key"]]

        if len(cls._primary_keys) == 1:
            dict_columns["pk"] = {
                "primary_key": True,
                "column": "id",
                "doc_string": "table primary key",
                "type": "#autoincrement#",
                "nullable": False,
                "read_only": True,
                "default": "#autoincrement#",
                "unique": True,
                "relationships": cls.relationships}
        else:
            dict_columns["pk"] = {
                "primary_key": True,
                "column": cls._primary_keys,
                "doc_string": "base64 encoded json dictionary",
                "type": "str",
                "nullable": False,
                "read_only": True,
                "default": None,
                "unique": True,
                "partition": cls.table_partition}
        return dict_columns

    def search_options(self):
        """Return search options for list pages."""
        return self.cls_search_options()

    def fill_options(self, partial_data, field=None):
        """Return fill options for retrieve/save pages."""
        return self.cls_search_options()


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

    def pivot(self, filter_dict: dict = {}, exclude_dict: dict = {},
              order_by: list = [], columns: list = [], format: str = 'list',
              variables: list = None, show_deleted: bool = False,
              add_pk_column: bool = False, limit: int = None,
              **kwargs):
        """
        Pivot end-point.

        Args:
            No args necessary.
        Kwargs:
            filter_dict (dict): Dictionary with the arguments to
                                be used in filter.
            exclude_dict (dict): Dictionary with the arguments to
                                 be used in exclude.
            order_by (list): List of fields to be used in ordering.
            columns (list): Columns to be used in pivoting
            format (str): Format to be used in pivot, same argument used in
                          pandas to_dict.
            variables (list) = []: List of the columns to be returned.
            show_deleted (bool) = False: If column deleted is available
                show deleted rows. By default those columns are removed.
            add_pk_column (bool): Add pk column to the results facilitating
                the pagination of long dataframes.
            limit (int) = None: Limit results to limit n rows.
        """
        model_variables = variables or self.model_variables
        if type(columns) != list:
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
        if hasattr(self.model_class, 'deleted'):
            if not show_deleted:
                filter_dict["deleted"] = False

        # Add pk/id columns to results
        if add_pk_column:
            if len(columns) != 0:
                raise exceptions.PumpWoodException(
                    "Can not add pk column and pivot information")
            for pk_col in self._primary_keys:
                if (pk_col not in model_variables):
                    model_variables = [pk_col] + model_variables

        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict
        to_function_dict['order_by'] = order_by
        query = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            **to_function_dict)

        # Limit results to help on pagination
        if limit is not None:
            query = query.limit(limit)

        # Set columns to be returned at query
        variables_to_return = [
            col for col in list(alchemy_inspect(self.model_class).c)
            if col.key in model_variables]

        query = query.with_entities(*variables_to_return)
        melted_data = pd.read_sql(query.statement, query.session.bind)

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
                pd.notnull(pivoted_table), None)
            response = pivoted_table.reset_index().to_dict(format)

        if type(response) == dict:
            response = {str(k): v for k, v in response.items()}
        return response

    def bulk_save(self, data_to_save: list):
        """
        Bulk save data.

        Args:
            data_to_save(list): List of dictionaries which must have
                                self.expected_cols_bulk_save.
        Return:
            dict: ['saved_count']: total of saved objects.
        """
        if len(self.expected_cols_bulk_save) == 0:
            raise exceptions.PumpWoodException('Bulk save not avaiable.')

        session = self.model_class.query.session
        pd_data_to_save = pd.DataFrame(data_to_save)
        pd_data_cols = set(list(pd_data_to_save.columns))

        objects_to_load = []
        if len(set(self.expected_cols_bulk_save) - pd_data_cols) == 0:
            for d in data_to_save:
                new_obj = self.model_class(**d)
                objects_to_load.append(new_obj)

            try:
                session.bulk_save_objects(objects_to_load)
                session.commit()
            except sqlalchemy.exc.IntegrityError as e:
                session.rollback()
                raise exceptions.PumpWoodIntegrityError(message=str(e))
            except psycopg2.errors.IntegrityError as e:
                session.rollback()
                raise exceptions.PumpWoodIntegrityError(message=str(e))
            except Exception as e:
                session.rollback()
                raise exceptions.PumpWoodException(message=str(e))

            return {'saved_count': len(objects_to_load)}
        else:
            template = 'Expected columns and data columns do not match:' + \
                '\nExpected columns:{expected}' + \
                '\nData columns:{data_cols}'
            raise exceptions.PumpWoodException(template.format(
                expected=set(self.expected_cols_bulk_save),
                data_cols=pd_data_cols,))


class PumpWoodDimensionsFlaskView(PumpWoodFlaskView):
    """Class view for models that hold data."""

    _view_type = "dimension"

    def dispatch_request(self, end_point, first_arg=None, second_arg=None):
        """dispatch_request for view, add pivot end point."""
        ###########################
        # Load payload from request
        data = None
        if request.method.lower() in ('post', 'put'):
            if request.mimetype == 'application/json':
                data = request.get_json()
            else:
                data = request.form.to_dict()
                for k in data.keys():
                    data[k] = json.loads(data[k])

        ########################
        #
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

        return super(PumpWoodDimensionsFlaskView, self).dispatch_request(
            end_point, first_arg, second_arg)

    def list_dimensions(self, filter_dict: dict = {},
                        exclude_dict: dict = {}) -> list:
        """List dimensions avaiable using query.

        Parameters
        ----------
        filter_dict : dict
            Filter query dict to get avaiable dimensions.
        exclude_dict : dict
            Exclude query dict to get avaiable dimensions.

        Returns
        -------
        List[str]
            List of the avaiable keys on dimensions database.
        """
        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        to_function_dict['filter_dict'] = filter_dict
        to_function_dict['exclude_dict'] = exclude_dict

        query_string = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            **to_function_dict).statement.compile(
                compile_kwargs={"literal_binds": True}).string
        distinct_keys = pd.read_sql("""
            SELECT DISTINCT jsonb_object_keys(dimensions) AS keys
            FROM (
                {query_string}
            ) sub
            ORDER BY keys
        """.format(query_string=query_string), con=self.db.engine)["keys"]
        return distinct_keys

    def list_dimension_values(self, key: str, filter_dict: dict = {},
                              exclude_dict: dict = {}) -> list:
        """List dimensions avaiable using query.

        Parameters
        ----------
        Args:
        key: str
            Key to list possible values in database.

        Kwargs:
        filter_dict: dict = None
            Filter query dict to get avaiable dimensions.
        exclude_dict: dict = None
            Exclude query dict to get avaiable dimensions.

        Returns
        -------
        List[str]
            List of the avaiable values for key dimention.
        """
        to_function_dict = {}
        to_function_dict['object_model'] = self.model_class
        if filter_dict is not None:
            to_function_dict["filter_dict"] = filter_dict
        if exclude_dict is not None:
            to_function_dict["exclude_dict"] = exclude_dict

        query_string = SqlalchemyQueryMisc.sqlalchemy_kward_query(
            **to_function_dict).statement.compile(
                compile_kwargs={"literal_binds": True}).string

        distinct_values = pd.read_sql("""
            SELECT DISTINCT dimensions -> '{key}' AS value
            FROM (
                {query_string}
            ) sub
            WHERE dimensions -> '{key}' IS NOT NULL
            ORDER BY value
        """.format(query_string=query_string, key=key),
            con=self.db.engine)["value"]
        return distinct_values


def register_pumpwood_view(app, view, service_object: dict):
    """
    Register a pumpwood view.

    Args:
        app (Flask App): Flask app to register the PumpWood View
        view (PumpWoodFlaskView or PumpWoodDataFlaskView): View to be
          registered
        suffix (str): Sufix to be added to the begging of the of the model
            name.
    Raises:
        No particular raises.

    """
    print("# Creating routes for [%s]" % view.model_class.__name__)
    view.create_route_object(service_object=service_object)

    model_class_name = view.model_class.__name__
    suffix = os.getenv('ENDPOINT_SUFFIX', '')
    model_class_name = suffix + model_class_name

    url_no_args = '/rest/%s/<end_point>/' % model_class_name.lower()
    url_1_args = '/rest/%s/<end_point>/<first_arg>/' % model_class_name.lower()
    url_2_args = '/rest/%s/<end_point>/<first_arg>/<second_arg>/' % \
        model_class_name.lower()

    view_func = view.as_view()
    app.add_url_rule(url_no_args, view_func=view_func)
    app.add_url_rule(url_1_args, view_func=view_func)
    app.add_url_rule(url_2_args, view_func=view_func)

    # Error handlers
    @app.errorhandler(exceptions.PumpWoodException)
    def handle_pumpwood_errors(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    # Python errors
    @app.errorhandler(TypeError)
    def handle_type_errors(error):
        pump_exc = exceptions.PumpWoodException(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    # SQLAlchemy errors
    @app.errorhandler(sqlalchemy.exc.ProgrammingError)
    def handle_sqlalchemy_programmingerror_errors(error):
        pump_exc = exceptions.PumpWoodException(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(sqlalchemy.exc.IntegrityError)
    def handle_sqlalchemy_invalidrequest_error(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(sqlalchemy.exc.InvalidRequestError)
    def handle_sqlalchemy_invalidrequest_error(error):
        pump_exc = exceptions.PumpWoodQueryException(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    # psycopg2 error handlers
    @app.errorhandler(psycopg2.errors.DatabaseError)
    def handle_psycopg2_DatabaseError(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(psycopg2.errors.OperationalError)
    def handle_psycopg2_OperationalError(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(psycopg2.errors.NotSupportedError)
    def handle_psycopg2_NotSupportedError(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(psycopg2.errors.ProgrammingError)
    def handle_psycopg2_ProgrammingError(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(psycopg2.errors.DataError)
    def handle_psycopg2_DataError(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    @app.errorhandler(psycopg2.errors.IntegrityError)
    def handle_psycopg2_IntegrityError(error):
        pump_exc = exceptions.PumpWoodDatabaseError(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response
