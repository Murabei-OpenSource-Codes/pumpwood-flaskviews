"""Pumpwood Marshmellow microservice fields."""
import copy
from loguru import logger
from typing import List, Dict, Any, Union
from marshmallow.fields import Field
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.config import (
    SERIALIZER_FK_CACHE_TIMEOUT)


class MicroserviceForeignKeyField(Field):
    """Serializer field for ForeignKey using microservice.

    Returns a tupple with both real value on [0] and get_{field_name}_display
    on [1]. to_internal_value uses only de first value os the tupple
    if a tupple, or just the value if not a tupple.
    """

    # Disable check if attribute exists on object, Micro service related are
    # not values on object
    _CHECK_ATTRIBUTE = False

    def __init__(self, source: str,
                 microservice: PumpWoodMicroService,
                 model_class: str, display_field: str = None,
                 complementary_source: Dict[str, str] = None,
                 fields: List[str] = None, **kwargs):
        """Class constructor.

        Args:
            source (str):
                Name of the field that contains foreign_key id.
            complementary_source (Dict[str, str]): = dict()
                When related field has a composite primary key it is
                necessary to specify complementary primary key field to
                fetch the object. The dictonary will set the mapping
                of the complementary pk field to correspondent related
                model obj key -> related object field.
            microservice (PumpWoodMicroService):
                Microservice object that will be used to retrieve
                foreign_key information.
            model_class (str):
                Model class associated with Foreign Key.
            display_field  (str):
                Display field that is set as __display_field__ value
                when returning the object.
            fields (List[str]):
                Set the fileds that will be returned at the foreign key
                object.
            extra_pk_fields
            **kwargs:
                Compatibylity with other versions and super of method.
        """
        complementary_source = (
            {} if complementary_source is None
            else complementary_source)

        # Validations
        if type(source) is not str:
            msg = "source argument must be a string"
            raise exceptions.PumpWoodOtherException(message=msg)
        if type(complementary_source) is not dict:
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
            object_data = self.microservice.list_one(
                model_class=self.model_class, pk=object_pk,
                fields=self.fields, use_disk_cache=True,
                disk_cache_expire=SERIALIZER_FK_CACHE_TIMEOUT)

        except exceptions.PumpWoodObjectDoesNotExist:
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodObjectDoesNotExist',
                "payload": {
                    "pk": object_pk,
                    "requester_username": user['username']}}

        except exceptions.PumpWoodUnauthorized:
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodUnauthorized',
                "__display_field__": (
                    "Your access token expired, login again."),
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class,
                    "requester_username": user['username']}}

        except exceptions.PumpWoodForbidden:
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodForbidden',
                "__display_field__": (
                    "You do not have access to this end-point"),
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class,
                    "requester_username": user['username']}}

        except Exception:
            user = AuthFactory.retrieve_authenticated_user()
            error_msg = (
                "Exception no caught when trying to retrieve FK using "
                "microservice.\n"
                "Object Model class and PK: [{model_class}] [{pk}]\n"
                "Username and PK:[{username}] [{user_id}]")\
                .format(
                    model_class=self.model_class, pk=object_pk,
                    username=user['username'], user_id=user['pk'])
            logger.exception(error_msg)
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodOtherException',
                "__display_field__": (
                    "Something went wrong, please contact support"),
                "payload": {
                    "pk": object_pk,
                    "model_class": self.model_class,
                    "requester_username": user['username']}}

        if self.display_field is not None:
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
        else:
            object_data['__display_field__'] = None
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
        # the front-end when always treating forenging key as a
        # dictonary/object field.
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
        return {
            'model_class': self.model_class, 'many': False,
            'display_field': self.display_field,
            'object_field': self.name, 'source_keys': source_keys}


class MicroserviceRelatedField(Field):
    """Serializer field for related objects using microservice.

    It is an informational serializer to related models.
    """

    _CHECK_ATTRIBUTE = False

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
                Set the fileds that will be returned at the foreign key
                object.
            read_only (bool):
                Not implemented yet. It will set if it is possible to create
                related objects using this end-point.
            **kwargs (dict):
                Dictonary if extra parameters to be used on function.
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
        """Use microservice to get object at serialization."""
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
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodObjectDoesNotExist',
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields,
                    "requester_username": user['username']}}

        except exceptions.PumpWoodUnauthorized:
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodUnauthorized',
                "__display_field__": (
                    "Your access token expired, login again."),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields,
                    "requester_username": user['username']}}

        except exceptions.PumpWoodForbidden:
            user = AuthFactory.retrieve_authenticated_user()
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodForbidden',
                "__display_field__": (
                    "You do not have access to this end-point"),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields,
                    "requester_username": user['username']}}

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
                "Fields: {fields}\n")\
                .format(
                    model_class=self.model_class,
                    username=user['username'],
                    user_id=user['pk'],
                    filter_dict=filter_dict,
                    exclude_dict=exclude_dict,
                    order_by=order_by,
                    fields=fields)
            logger.exception(error_msg)
            return {
                "model_class": self.model_class,
                "__error__": 'PumpWoodOtherException',
                "__display_field__": (
                    "Something went wrong, please contact support"),
                "payload": {
                    "filter_dict": filter_dict, "exclude_dict": exclude_dict,
                    "order_by": order_by, "fields": fields,
                    "requester_username": user['username']}}

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "MicroserviceRelatedField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        return {
            'model_class': self.model_class, 'many': True,
            'pk_field': self.pk_field,
            'foreign_key': self.foreign_key,
            'complementary_foreign_key': self.complementary_foreign_key,
            'order_by': self.order_by,
            'fields': self.fields}
