"""Pumpwood Marshmellow microservice fields."""
import copy
from dataclasses import dataclass
from loguru import logger
from typing import List, Dict, Any, Union
from marshmallow.fields import Field, Integer
from pumpwood_communication import exceptions
from pumpwood_communication.exceptions import raise_from_dict
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_communication.type import (
    ForeignKeyColumnExtraInfo, RelatedColumnExtraInfo,
    PumpwoodDataclassMixin)
from pumpwood_flaskviews.query.aux import get_base_filter_skip
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.cache import PumpwoodFlaskGCache
from pumpwood_flaskviews.config import SERIALIZER_FK_CACHE_TIMEOUT


@dataclass
class MicroserviceForeignKeyFieldCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for MicroserviceForeignKeyField.

    This dataclass is used to generate a unique hash for caching foreign
    key data retrieved from microservices, ensuring that authorization
    and field selection are part of the cache key.
    """

    authorization_token: str
    """Request authorization token."""
    model_class: str
    """Model class for the autofill field."""
    object_pk: str | int
    """Pk associated with object to get the autofill field data."""
    fields: str | None
    """Field to extract data to fill object."""
    context: str = 'pumpwood-flaskviews-microservice-foreignkey-field'
    """Context identifier for the cache entry."""


class MicroserviceForeignKeyField(Field):
    """Serializer field for ForeignKey using microservice.

    Returns a dictionary with the object data retrieved from the
    microservice. This field is read-only and is used to provide
    related object information during serialization.
    """

    # Disable check if attribute exists on object, Micro service related are
    # not values on object
    _CHECK_ATTRIBUTE = False
    _PUMPWOOD_FK = True
    """Set _PUMPWOOD_FK=True, this will be used by serializer to get if this
       field is a 'Foreign Key'."""

    def __init__(self, source: str,
                 microservice: PumpWoodMicroService,
                 model_class: str, display_field: str = None,
                 complementary_source: Dict[str, str] = None,
                 fields: List[str] = None, **kwargs):
        """Class constructor.

        Args:
            source (str):
                Name of the field that contains foreign_key id.
            microservice (PumpWoodMicroService):
                Microservice object that will be used to retrieve
                foreign_key information.
            model_class (str):
                Model class associated with Foreign Key.
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
        if not isinstance(source, str):
            msg = "source argument must be a string"
            raise exceptions.PumpWoodOtherException(message=msg)
        if not isinstance(complementary_source, dict):
            msg = "complementary_source argument must be a dictonary"
            raise exceptions.PumpWoodOtherException(message=msg)

        self.microservice = microservice
        self.model_class = model_class
        self.display_field = display_field
        self.complementary_source = complementary_source
        self.source = source
        self.fields = fields

        # Set as read only and not required, changes on foreign key must be
        # done using id
        kwargs['required'] = False
        kwargs['dump_only'] = True
        super(MicroserviceForeignKeyField, self).__init__(**kwargs)

    def get_source_pk_fields(self) -> List[str]:
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

    def _microservice_retrieve(self, object_pk: Union[int, str],
                               fields: List[str]) -> dict:
        """Retrieve data using microservice and cache results.

        Retrieve data using list_one at the destination model_class, it
        will cache the results on request object to reduce processing time.

        Args:
            object_pk (Union[int, str]):
                Object primary key to retrieve information using
                microservice.
            fields (List[str]):
                Limit the fields that will be returned using microservice.

        Returns:
            dict:
                A dictionary containing the object data or error metadata.
        """
        hash_dict = MicroserviceForeignKeyFieldCacheHash(
            authorization_token=AuthFactory.get_auth_header()['Authorization'],
            model_class=self.model_class, object_pk=object_pk,
            fields=self.fields)
        g_cached_data = PumpwoodFlaskGCache.get(hash_dict=hash_dict)
        if g_cached_data is not None:
            return g_cached_data

        # If cache not found, retrieve data using microservice. At retrieve
        # call on pumpwood it will also try to retrieve information
        # from disk cache if avaiable.
        is_error = False
        try:
            object_data = self.microservice.list_one(
                model_class=self.model_class, pk=object_pk,
                fields=self.fields, use_disk_cache=True,
                disk_cache_expire=SERIALIZER_FK_CACHE_TIMEOUT)

        except exceptions.PumpWoodObjectDoesNotExist:
            is_error = True
            object_data = {
                "model_class": self.model_class,
                "pk": object_pk,
                "__error__": 'PumpWoodObjectDoesNotExist',
                "__display_field__": "Object not found",
                "payload": {
                    "pk": object_pk}}

        except exceptions.PumpWoodUnauthorized:
            is_error = True
            object_data = {
                "model_class": self.model_class,
                "pk": object_pk,
                "__error__": 'PumpWoodUnauthorized',
                "__display_field__": (
                    "Your access token expired, login again."),
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class}}

        except exceptions.PumpWoodForbidden:
            is_error = True
            object_data = {
                "model_class": self.model_class,
                "pk": object_pk,
                "__error__": 'PumpWoodForbidden',
                "__display_field__": (
                    "You do not have access to this end-point"),
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class}}

        except Exception:
            is_error = True
            user = AuthFactory.retrieve_authenticated_user()
            error_msg = (
                "Exception no caught when trying to retrieve FK using "
                "microservice.\n"
                "Object Model class and PK: [{model_class}] [{pk}]\n"
                "Username and PK:[{username}] [{user_id}]")
            logger.exception(
                error_msg,
                model_class=self.model_class, pk=object_pk,
                username=user['username'], user_id=user['pk'])
            object_data = {
                "model_class": self.model_class,
                "pk": object_pk,
                "__error__": 'PumpWoodOtherException',
                "__display_field__": (
                    "Something went wrong, please contact support"),
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class}}

        # Add display field to facilitate frontend development
        if self.display_field is not None and not is_error:
            if self.display_field not in object_data.keys():
                msg = (
                    "Serializer not correctly configured, it is not possible "
                    "to find display_field[{display_field}] at the object "
                    "of foreign_key[{foreign_key}] liked to "
                    "model_class[{model_class}]").format(
                        display_field=self.display_field,
                        foreign_key=self.name, model_class=self.model_class)
                raise exceptions.PumpWoodOtherException(
                    msg, payload={
                        "display_field": self.display_field,
                        "foreign_key": self.name,
                        "model_class": self.model_class})
            object_data['__display_field__'] = object_data[self.display_field]
        elif not is_error:
            object_data['__display_field__'] = None

        # Set g object cache to reduce disk cache calls
        PumpwoodFlaskGCache.set(
            hash_dict=hash_dict, value=object_data)
        return object_data

    def _serialize(self, value, attr, obj, **kwargs):
        """Use microservice to get object at serialization."""
        self.microservice.login()

        object_pk = None
        if not self.complementary_source:
            object_pk = getattr(obj, self.source)
        else:
            primary_keys = {self.source: 'id'}
            primary_keys.update(self.complementary_source)
            object_pk = CompositePkBase64Converter.dump(
                obj=obj, primary_keys=primary_keys)

        # Return an empty object if object pk is None, this will help
        # the front-end when always treating foreign key as a
        # dictionary/object field.
        if object_pk is None:
            return {"model_class": self.model_class}
        return self._microservice_retrieve(
            object_pk=object_pk, fields=self.fields)

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "MicroserviceForeignKeyField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        source_keys = self.get_source_pk_fields()
        fk_type_obj = ForeignKeyColumnExtraInfo(
            model_class=self.model_class, many=False,
            display_field=self.display_field, object_field=self.name,
            source_keys=source_keys)
        return fk_type_obj


class MicroserviceRelatedField(Field):
    """Serializer field for related objects using microservice.

    It is an informational serializer to related models.
    """

    _CHECK_ATTRIBUTE = False
    _PUMPWOOD_RELATED = True
    """Set _PUMPWOOD_FK=True, this will be used by serializer to get if this
       field is a 'Related Field'."""

    def __init__(self, microservice: PumpWoodMicroService,
                 model_class: str, foreign_key: str,
                 complementary_foreign_key: None | Dict[str, str] = None,
                 pk_field: str = 'id', order_by: List[str] = None,
                 exclude_dict: None | Dict[str, str] = None,
                 help_text: str = "", read_only: bool = True,
                 fields: List[str] = None, **kwargs):
        """Class constructor.

        Args:
            microservice (PumpWoodMicroService):
                Microservice object that will be used to retrieve
                foreign_key information.
            model_class (str):
                Model class associated with Foreign Key.
            foreign_key (str):
                Foreign Key field that is a foreign key id to origin
                model class.
            complementary_foreign_key (Dict[str, str]):
                Complementary primary key fields that will be used on query
                to reduce query time.
            pk_field (str):
                Field of the origin model class that will be used to filter
                related models at foreign_key.
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

        # Validation
        if type(complementary_foreign_key) is not dict:
            msg = "complementary_foreign_key type must be a dict"
            raise exceptions.PumpWoodOtherException(message=msg)
        if type(foreign_key) is not str:
            msg = "foreign_key type must be a str"
            raise exceptions.PumpWoodOtherException(message=msg)
        if type(order_by) is not list:
            msg = "order_by type must be a list"
            raise exceptions.PumpWoodOtherException(message=msg)
        if type(exclude_dict) is not dict:
            msg = "exclude_dict type must be a dict"
            raise exceptions.PumpWoodOtherException(message=msg)

        self.microservice = microservice
        self.model_class = model_class
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
        super(MicroserviceRelatedField, self).__init__(**kwargs)

    def _get_list_arg_filter_dict(self, obj) -> Dict[str, Any]:
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

        Cache is not used on this serialization since this type of field
        is only used for `many=False` serializations. It will only serialize
        one object with one related field, which will lead to G cache calls
        to be not used (just one object, no other to retrieve the cache.)
        """
        self.microservice.login()
        filter_dict = self._get_list_arg_filter_dict(obj)
        exclude_dict = self._get_list_arg_exclude_dict(obj)
        order_by = self._get_list_arg_order_by(obj)
        fields = self._get_list_arg_fields(obj)

        try:
            return self.microservice.list_without_pag(
                model_class=self.model_class,
                filter_dict=filter_dict, exclude_dict=exclude_dict,
                order_by=order_by, fields=fields,
                default_fields=True)

        except exceptions.PumpWoodObjectDoesNotExist:
            return [{
                "model_class": self.model_class,
                "__error__": 'PumpWoodObjectDoesNotExist',
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields}}]

        except exceptions.PumpWoodUnauthorized:
            return [{
                "model_class": self.model_class,
                "__error__": 'PumpWoodUnauthorized',
                "__display_field__": (
                    "Your access token expired, login again."),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields}}]

        except exceptions.PumpWoodForbidden:
            return [{
                "model_class": self.model_class,
                "__error__": 'PumpWoodForbidden',
                "__display_field__": (
                    "You do not have access to this end-point"),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields}}]

        except Exception:
            user = AuthFactory.retrieve_authenticated_user()
            error_msg = (
                "Exception no caught when trying to retrieve related using "
                "microservice.\n"
                "Object Model class and PK: [{model_class}]\n"
                "Username and PK:[{username}] [{user_id}]\n"
                "Filter dict: {filter_dict}\n"
                "Exclude dict: {exclude_dict}\n"
                "Order by: {order_by}\n"
                "Fields: {fields}")
            logger.exception(
                error_msg,
                model_class=self.model_class, username=user['username'],
                user_id=user['pk'], filter_dict=filter_dict,
                exclude_dict=exclude_dict, order_by=order_by,
                fields=fields)
            return [{
                "model_class": self.model_class,
                "__error__": 'PumpWoodOtherException',
                "__display_field__": (
                    "Something went wrong, please contact support"),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields}}]

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "MicroserviceRelatedField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        return RelatedColumnExtraInfo(
            model_class=self.model_class, many=True, pk_field=self.pk_field,
            foreign_key=self.foreign_key,
            complementary_foreign_key=self.complementary_foreign_key,
            fields=self.fields)


class ValidateForeignKeyFieldMicroservice(Integer):
    """Integer FK field that validates access via microservice retrieve.

    Deserializes the value as an integer primary key and checks that
    the related row exists and is visible for the current user.
    """

    def __init__(self, *args, model_class: str,
                 not_logged_microservice: PumpWoodMicroService, **kwargs):
        """Class constructor.

        Args:
            model_class (str):
                Model class name for the foreign key target on the
                remote microservice.
            not_logged_microservice (PumpWoodMicroService):
                Microservice client used to retrieve the related
                object. Request auth is forwarded via auth_header.
            *args:
                Positional arguments forwarded to IntField.
            **kwargs:
                Keyword arguments forwarded to IntField.
        """
        if not isinstance(model_class, str):
            msg = (
                "Serializer for {name} model_class argument must be a "
                "string.").format(name=self.__class__.__name__)
            raise exceptions.PumpWoodOtherException(message=msg)

        self.model_class = model_class
        self.microservice = not_logged_microservice
        super().__init__(*args, **kwargs)

    def _validate_obj_access(self, object_pk: str | int) -> None:
        """Validate if user has access to object.

        Args:
            object_pk (str | int):
                Primary key of the related object.

        Raises:
            PumpWoodObjectDoesNotExist:
                If the object was not found or is not accessible.
            PumpWoodUnauthorized:
                If the request auth token is invalid or expired.
            PumpWoodForbidden:
                If the user lacks permission to access the object.
        """
        auth_header = AuthFactory.get_auth_header()
        # Use just the pk as field to retrieve the object since it is not
        # necessary to retrieve the object data, just the access.
        hash_dict = MicroserviceForeignKeyFieldCacheHash(
            authorization_token=auth_header['Authorization'],
            model_class=self.model_class, object_pk=object_pk,
            fields=['pk'])
        g_cached_data = PumpwoodFlaskGCache.get(hash_dict=hash_dict)
        if g_cached_data is not None:
            # Check if it is an error and raise it.
            error_type = g_cached_data.get('__error__')
            if error_type is not None:
                # Will raise the exception from the dictionary using default
                # Pumpwood exception classes.
                raise_from_dict(exception_dict=g_cached_data)
            return g_cached_data
        
        # If cache not found, retrieve data using microservice.
        try:
            # Propagate the base_filter_skip to validate access to FK
            base_filter_skip = get_base_filter_skip()
            obj = self.microservice.retrieve(
                model_class=self.model_class, pk=object_pk,
                use_disk_cache=True, auth_header=auth_header, fields=['pk'],
                base_filter_skip=base_filter_skip)
        except exceptions.PumpWoodException as e:
            # To dict will create a dictionary with the error information,
            # set the cache and raise the exception.
            error_dict = e.to_dict()
            PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=error_dict)
            raise e

        PumpwoodFlaskGCache.set(hash_dict=hash_dict, value=obj)

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
            PumpWoodUnauthorized:
                If the request auth token is invalid or expired.
            PumpWoodForbidden:
                If the user lacks permission to access the object.
        """
        val = super()._deserialize(value, attr, data, **kwargs)
        self._validate_obj_access(object_pk=val)
        return val