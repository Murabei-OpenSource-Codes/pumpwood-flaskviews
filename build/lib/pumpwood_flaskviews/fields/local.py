"""Pumpwood Marshmellow local reference fields."""
import copy
from dataclasses import dataclass
from loguru import logger
from typing import List, Dict, Any, Union
from marshmallow.fields import Field, IntField
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Mapper, InstanceState
from sqlalchemy.exc import NoInspectionAvailable
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.type import (
    ForeignKeyColumnExtraInfo, RelatedColumnExtraInfo,
    PumpwoodDataclassMixin)
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.model import FlaskPumpWoodBaseModel
from pumpwood_flaskviews.fields.aux import _import_function_by_string
from pumpwood_flaskviews.cache import PumpwoodFlaskGDiskCache


def _get_sqlalchemy_type(obj: Any) -> str:
    """Return a string identifying the type of the SQLAlchemy object.

    Args:
        obj (Any):
            An object to check if it is a SQLAlchemy object.

    Returns:
        Return "instance" if if object is an instance, "class" if it is a
        class and None if not an SQLAlchemy object.
    """
    try:
        inspected = inspect(obj)
        if isinstance(inspected, Mapper):
            return "class"
        elif isinstance(inspected, InstanceState):
            return "instance"
    except NoInspectionAvailable:
        return "not_sqlalchemy"



class LocalForeignKeyField(Field):
    """Serializer field for ForeignKey using local query.

    Query connection to retrieve object from database and serialize it
    using defined serializer.
    """

    # Disable check if attribute exists on object, Micro service related are
    # not values on object
    _CHECK_ATTRIBUTE = False
    _PUMPWOOD_FK = True
    """Set _PUMPWOOD_FK=True, this will be used by serializer to get if this
       field is a 'Foreign Key'."""

    def __init__(self, source: str,
                 model_class: str | FlaskPumpWoodBaseModel,
                 serializer: str | object,
                 display_field: str = None,
                 complementary_source: Dict[str, str] = None,
                 fields: List[str] = None, **kwargs):
        """Class constructor.

        Args:
            source (str):
                Name of the field that contains foreign_key id.
            model_class (str | FlaskPumpWoodBaseModel):
                Local model class from which information will be retrieved,
                it is possible to use string to avoid circular imports.
            serializer (str | PumpWoodSerializer):
                Serializer that will be used to serialize objects, it is
                possible to use a string to avoid circular imports.
            display_field (str):
                Display field that is set as __display_field__ value
                when returning the object.
            complementary_source (Dict[str, str]):
                When related field has a composite primary key it is
                necessary to specify complementary primary key field to
                fetch the object. The dictionary will set the mapping
                of the complementary pk field to correspondent related
                model obj key -> related object field.
            fields (List[str]):
                Set the fields that will be returned at the foreign key
                object.
            **kwargs:
                Compatibility with other versions and super of method.
        """
        complementary_source = (
            {} if complementary_source is None
            else complementary_source)

        # Validations
        if not isinstance(source, (str)):
            msg = (
                "Serializer for {name} source argument must be a string")\
                .format(name=self.__name__)
            raise exceptions.PumpWoodOtherException(message=msg)

        if not isinstance(model_class, str):
            sqlalchemy_type = _get_sqlalchemy_type(obj=model_class)
            if sqlalchemy_type != "class":
                msg = (
                    "Serializer for {name} model_class argument must be a "
                    "string or FlaskPumpWoodBaseModel.").format(
                        name=self.__class__.__name__)
                raise exceptions.PumpWoodOtherException(message=msg)

        if not isinstance(complementary_source, (dict)):
            msg = (
                "Serializer for {name} complementary_source argument must "
                "be a dictionary or None").format(name=model_class)
            raise exceptions.PumpWoodOtherException(message=msg)

        # Set model_class and serializer as None at the object creation to
        # avoid circular dependency at app startup, this object will latter
        # be loaded at serialization
        self.model_class = None
        self.serializer = None
        self._pre_load_model_class = model_class
        self._pre_load_serializer = serializer

        self.display_field = display_field
        self.complementary_source = complementary_source
        self.source = source
        self.fields = fields

        # Set as read only and not required, changes on foreign key must be
        # done using id
        kwargs['required'] = False
        kwargs['dump_only'] = True
        super(LocalForeignKeyField, self).__init__(**kwargs)

    def _load_model_class(self) -> FlaskPumpWoodBaseModel:
        """Load model class at serialization to avoid circular dependency."""
        if self.model_class is None:
            self.model_class = _import_function_by_string(
                module=self._pre_load_model_class)

    def _load_serializer(self) -> object:
        """Load model class at serialization to avoid circular dependency."""
        if self.serializer is None:
            self.serializer = _import_function_by_string(
                module=self._pre_load_serializer)

    def _get_source_pk_fields(self) -> List[str]:
        """Return a list of source fields associated with FK.

        If will return the source pk and the complementary_source
        keys.

        Args:
            No Args.

        Returns:
            Return a list of the fields that are considered when retrieving
            a foreign key.
        """
        # Treat when complementary_source is not set
        complementary_source = self.complementary_source | {}
        return [self.source] + list(complementary_source.keys())

    def _retrieve_data(self, object_pk: Union[int, str],
                       fields: List[str]) -> dict:
        """Retrieve data using microservice and cache results.

        Retrieve data using list one at the destination model_class, it
        will cache de results on request object to reduce processing time.

        Args:
            object_pk (Union[int, str]):
                Object primary key to retrieve information using
                microservice.
            fields (List[str]):
                Limit the fields that will be returned using microservice.
        """
        try:
            obj = self.model_class.default_query_get(pk=object_pk)

        except exceptions.PumpWoodObjectDoesNotExist:
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "pk": object_pk,
                "__display_field__": "Object not found",
                "__error__": 'PumpWoodObjectDoesNotExist',
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class}}

        except Exception:
            user = AuthFactory.retrieve_authenticated_user()
            error_msg = (
                "Exception no caught when trying to retrieve FK using "
                "local.\n"
                "Object Model class and PK: [{model_class}] [{pk}]\n"
                "Username and PK:[{username}] [{user_id}]")
            logger.exception(
                error_msg,
                model_class=self.model_class, pk=object_pk,
                username=user['username'], user_id=user['pk'])
            return {
                "model_class": self.model_class,
                "pk": object_pk,
                "__display_field__": (
                    "Something went wrong, please contact support"),
                "__error__": 'PumpWoodOtherException',
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class}}

        temp_serializer = self.serializer(
            many=False, fields=fields, default_fields=True)
        return temp_serializer.dump(obj)

    def _set_display_field(self, object_data: dict) -> dict:
        """Add a __display_field__ to object data.

        Args:
            object_data (dict):
                Object data to add `__display_field__` if field
                self.display_field is set.
        """
        if self.display_field is not None:
            object_data['__display_field__'] = \
                object_data.get(self.display_field)
        return object_data

    def _serialize(self, value, attr, obj, **kwargs):
        """Use microservice to get object at serialization."""
        # Lazy load model class and serializer to avoid circular dependency
        # when loading from str
        self._load_model_class()
        self._load_serializer()

        # Create object_pk to be used at retrieve data and cache
        object_pk = None
        if not self.complementary_source:
            object_pk = getattr(obj, self.source)
        else:
            primary_keys = {self.source: 'id'}
            primary_keys.update(self.complementary_source)
            object_pk = CompositePkBase64Converter.dump(
                obj=obj, primary_keys=primary_keys)

        # When the foreign_keys are None (not set), return a None
        # for object
        if object_pk is None:
            return None

        # If cache for this auth header is not avaible, fetch from database
        # and serialize. Then set the cache
        data_result = self._retrieve_data(
            object_pk=object_pk, fields=self.fields)
        is_error = data_result.get('__error__') is not None
        if not is_error:
            data_result = self._set_display_field(object_data=data_result)
        return data_result

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "LocalRelatedField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        self._load_model_class()
        source_keys = self._get_source_pk_fields()
        return ForeignKeyColumnExtraInfo(
            model_class=self.model_class.__name__, many=False,
            display_field=self.display_field, object_field=self.name,
            source_keys=source_keys)


class LocalRelatedField(Field):
    """Serializer field for related objects using microservice.

    It is an informational serializer to related models.
    """

    _CHECK_ATTRIBUTE = False
    _PUMPWOOD_RELATED = True
    """Set _PUMPWOOD_FK=True, this will be used by serializer to get if this
       field is a 'Related Field'."""

    def __init__(self,
                 model_class: str | FlaskPumpWoodBaseModel,
                 serializer: str | Any,
                 foreign_key: str,
                 complementary_foreign_key: None | Dict[str, str] = None,
                 pk_field: str = 'id', order_by: List[str] = None,
                 exclude_dict: None | Dict[str, str] = None,
                 help_text: str = "", read_only: bool = True,
                 fields: List[str] = None, **kwargs):
        """Class constructor.

        Args:
            model_class (str | FlaskPumpWoodBaseModel):
                Local model class from which information will be retrieved,
                it is possible to use string to avoid circular imports.
            serializer (str | FlaskPumpWoodBaseModel):
                Serializer that will be used serialize objects, it is possible
                to use a string to avoid circular imports.
            foreign_key (str):
                Foreign Key field that is a foreign key id to origin
                model class.
            complementary_foreign_key (Dict[str, str]):
                Complementary primary key fields that will be used on query
                to reduce query time.
            pk_field (str):
                Field of the origin model class that will be used to filter
                related models at foreign_key.
            display_field (str):
                Display field that is set as __display_field__ value
                when returning the object.
            order_by (List[str]):
                List of strings that will be used to order query results.
            exclude_dict (Dict[str, str]):
                Default exclude_dict to be applied at list end-point to
                retrieve related objects.
            help_text (str):
                Help text associated with related model. This will be
                returned at fill_options data.
            fields (List[str]):
                Set the fields that will be returned at the foreign key
                object.
            read_only (bool):
                Not implemented yet. It will set if it is possible to create
                related objects using this end-point.
            **kwargs (dict):
                Dictionary of extra parameters to be used on function.
        """
        complementary_foreign_key = (
            {} if complementary_foreign_key is None
            else complementary_foreign_key)
        order_by = (
            ["-id"] if order_by is None
            else order_by)
        exclude_dict = (
            {} if exclude_dict is None
            else exclude_dict)

        # Validations
        if not isinstance(foreign_key, (str)):
            msg = (
                "{name} source argument must be a string or")\
                    .format(name=self.__name__)
            raise exceptions.PumpWoodOtherException(message=msg)

        if not isinstance(model_class, str):
            sqlalchemy_type = _get_sqlalchemy_type(obj=model_class)
            if sqlalchemy_type != "class":
                msg = (
                    "Serializer for {name} model_class argument must be a "
                    "string or FlaskPumpWoodBaseModel.").format(
                        name=model_class)
                raise exceptions.PumpWoodOtherException(message=msg)

        if not isinstance(complementary_foreign_key, (dict)):
            msg = (
                "{name} complementary_source argument must be a dictonary "
                "or None").format(name=self.__name__)
            raise exceptions.PumpWoodOtherException(message=msg)

        if not isinstance(foreign_key, str):
            msg = "foreign_key type must be a str"
            raise exceptions.PumpWoodOtherException(message=msg)
        if not isinstance(complementary_foreign_key, dict):
            msg = "complementary_foreign_key type must be a dict"
            raise exceptions.PumpWoodOtherException(message=msg)
        if not isinstance(order_by, list):
            msg = "order_by type must be a list"
            raise exceptions.PumpWoodOtherException(message=msg)
        if not isinstance(exclude_dict, dict):
            msg = "exclude_dict type must be a dict"
            raise exceptions.PumpWoodOtherException(message=msg)

        # Set model_class and serializer as None at the object creation to
        # avoid circular dependency at app startup, this object will latter
        # be loaded at serialization
        self.model_class = None
        self.serializer = None
        self._pre_load_model_class = model_class
        self._pre_load_serializer = serializer

        # Information for object serialization
        self.foreign_key = foreign_key
        self.complementary_foreign_key = complementary_foreign_key
        self.pk_field = pk_field
        self.order_by = order_by
        self.exclude_dict = exclude_dict
        self.fields = fields

        # Informational data for options end-point
        self.help_text = help_text
        self.read_only = read_only

        # Force field not be necessary for saving object
        kwargs['required'] = False
        kwargs['dump_only'] = True

        # Set as read only and not required, changes on foreign key must be
        # done using id
        super().__init__(**kwargs)

    def _load_model_class(self) -> FlaskPumpWoodBaseModel:
        """Load model class at serialization to avoid circular dependency."""
        if self.model_class is None:
            self.model_class = _import_function_by_string(
                module=self._pre_load_model_class)

    def _load_serializer(self) -> object:
        """Load model class at serialization to avoid circular dependency."""
        if self.serializer is None:
            self.serializer = _import_function_by_string(
                module=self._pre_load_serializer)

    def _get_list_arg_filter_dict(self, obj: Any) -> Dict[str, Any]:
        """Return the filter_dict that will be used at list end-point.

        Returns:
            Return a dictionary that will be used on filter_dict at
            list end-point.
        """
        pk_field = getattr(obj, self.pk_field)
        filter_dict = {self.foreign_key: pk_field}
        for key, item in self.complementary_foreign_key.items():
            filter_dict[item] = getattr(obj, key)
        return filter_dict

    def _get_list_arg_exclude_dict(self, obj) -> Dict[str, Any]:
        """Return the exclude dict that will be used at list end-point.

        Returns:
            Return a dictionary that will be used as exclude_dict at
            list end-point.
        """
        return copy.deepcopy(self.exclude_dict)

    def _get_list_arg_order_by(self, obj) -> List[str]:
        """Return order_by list to be used at list end-point.

        Returns:
            Return a list that will be used as order_by at
            list end-point.
        """
        return copy.deepcopy(self.order_by)

    def _get_list_arg_fields(self, obj) -> List[str]:
        """Return fields list to be used at list end-point.

        Returns:
            Return a list that will be used as fields at
            list end-point.
        """
        return copy.deepcopy(self.fields)

    def _serialize(self, value, attr, obj, **kwargs):
        """Use microservice to get object at serialization.

        Related does not use cache since it will be used for seriailizing
        an unique object. Caching using G object does not make sense since
        it will not have another posterior object to consume this information
        from cache.
        """
        # Load model_class and serializer at the begginng of the serialization
        self._load_model_class()
        self._load_serializer()

        # Use functions to retrieve parameters to the query
        filter_dict = self._get_list_arg_filter_dict(obj)
        exclude_dict = self._get_list_arg_exclude_dict(obj)
        order_by = self._get_list_arg_order_by(obj)
        fields = self._get_list_arg_fields(obj)

        try:
            query_result = self.model_class.default_query_list(
                filter_dict=filter_dict, exclude_dict=exclude_dict,
                order_by=order_by)
        except Exception:
            user = AuthFactory.retrieve_authenticated_user()
            error_msg = (
                "Exception no caught when trying to retrieve related using "
                "local.\n"
                "Object Model class and PK: [{model_class}]\n"
                "Username and PK:[{username}] [{user_id}]\n"
                "Filter dict: {filter_dict}\n"
                "Exclude dict: {exclude_dict}\n"
                "Order by: {order_by}\n"
                "Fields: {fields}")
            logger.exception(
                error_msg, model_class=self.model_class,
                username=user['username'], user_id=user['pk'],
                filter_dict=filter_dict, exclude_dict=exclude_dict,
                order_by=order_by, fields=fields)
            return [{
                "model_class": self.model_class,
                "__error__": 'PumpWoodOtherException',
                "__display_field__": (
                    "Something went wrong, please contact support"),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields}}]

        list_serializer = self.serializer(
            many=True, fields=fields, default_fields=True)
        return list_serializer.dump(query_result, many=True)

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "LocalRelatedField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        self._load_model_class()
        return RelatedColumnExtraInfo(
            model_class=self.model_class.__name__,
            many=True,
            pk_field=self.pk_field,
            foreign_key=self.foreign_key,
            complementary_foreign_key=self.complementary_foreign_key,
            fields=self.fields)


class ValidateForeignKeyFieldLocal(IntField):
    """Integer FK field that validates user access to referenced object.

    Deserializes the value as an integer primary key and checks that
    the related row exists and is visible through default_query_get.
    """

    def __init__(self, *args, model_class: str | FlaskPumpWoodBaseModel,
                 **kwargs):
        """Class constructor.

        Args:
            model_class (str | FlaskPumpWoodBaseModel):
                Local model class for the foreign key target. A string
                import path avoids circular imports at app startup.
            *args:
                Positional arguments forwarded to IntField.
            **kwargs:
                Keyword arguments forwarded to IntField.
        """
        if not isinstance(model_class, str):
            sqlalchemy_type = _get_sqlalchemy_type(obj=model_class)
            if sqlalchemy_type != "class":
                msg = (
                    "Serializer for {name} model_class argument must be a "
                    "string or FlaskPumpWoodBaseModel.").format(
                        name=self.__class__.__name__)
                raise exceptions.PumpWoodOtherException(message=msg)

        self.model_class = None
        self._pre_load_model_class = model_class
        super().__init__(*args, **kwargs)

    def _get_model_class(self) -> FlaskPumpWoodBaseModel:
        """Load model class at serialization to avoid circular dependency.

        Returns:
            FlaskPumpWoodBaseModel:
                Resolved model class for the foreign key target.
        """
        if self.model_class is None:
            self.model_class = _import_function_by_string(
                module=self._pre_load_model_class)
        return self.model_class

    def _validate_obj_access(self, object_pk: str | int) -> None:
        """Validate if user has access to object.

        Args:
            object_pk (str | int):
                Primary key of the related object.

        Raises:
            PumpWoodObjectDoesNotExist:
                If the object was not found or is not accessible.
        """
        model_class = self._get_model_class()
        model_class.default_query_get(
            pk=object_pk, raise_error=True, use_cache=True)

    def _deserialize(self, value, attr, data, **kwargs):
        """Deserialize integer FK and validate related object access.

        Args:
            value:
                Raw input value for the field.
            attr (str):
                Attribute name on the schema.
            data (dict):
                Full input data dictionary.

        Returns:
            int:
                Deserialized primary key value.

        Raises:
            PumpWoodObjectDoesNotExist:
                If the related object was not found or is not accessible.
        """
        val = super()._deserialize(value, attr, data, **kwargs)
        self._validate_obj_access(object_pk=val)
        return val