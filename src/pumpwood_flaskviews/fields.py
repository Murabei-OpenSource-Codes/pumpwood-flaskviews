import base64
import importlib
import simplejson as json
from geoalchemy2.shape import from_shape, to_shape
from shapely import geometry
from marshmallow import fields
from sqlalchemy import inspect as sqlalchemy_inspect
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_communication.microservices import PumpWoodMicroService


def _import_function_by_string(module_function_string):
    """Help when importing a function using a string."""
    # Split the module and function names
    module_name, function_name = module_function_string.rsplit('.', 1)
    # Import the module
    module = importlib.import_module(module_name)
    # Retrieve the function
    func = getattr(module, function_name)
    return func


# Robado de @om-henners om-henners/serializer_utils.py
class GeometryField(fields.Field):
    """
    Create a marshmallow field to recieve geometry data.

    Use shapely and geoalchemy2 to serialize / deserialize a point
    Does make a big assumption about the data being spat back out as
    JSON, but what the hey.
    """

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        return geometry.mapping(to_shape(value))

    def _deserialize(self, value, attr, data):
        if value is None:
            return None
        return from_shape(geometry.shape(value), srid=4326)


# Choice field
class ChoiceField(fields.Field):
    """Create a marshmallow field to serialize ChoiceFields."""

    def _serialize(self, value, attr, obj):
        if value is not None:
            return value.code
        return None

    def _deserialize(self, value, attr, data):
        # Not checking if value is a string breaks saving the object.
        if type(value) is str:
            return value
        else:
            return value.code


# Primary Key Field
class PrimaryKeyField(fields.Function):
    """Create a marshmallow field to serialize ChoiceFields."""

    _primary_keys = None

    def _serialize(self, value, attr, obj, **kwargs):
        if self._primary_keys is None:
            mapper = sqlalchemy_inspect(obj.__table__)
            self._primary_keys = [
                col.name for col in list(mapper.c) if col.primary_key]
        return CompositePkBase64Converter.dump(
            obj=obj, primary_keys=self._primary_keys)

    def _deserialize(self, value, attr, data, **kwargs):
        return CompositePkBase64Converter.load(value=value)


####################################
# Microservice related serializers #
class MicroserviceForeignKeyField(fields.Field):
    """
    Serializer field for ForeignKey using microservice.

    Returns a tupple with both real value on [0] and get_{field_name}_display
    on [1]. to_internal_value uses only de first value os the tupple
    if a tupple, or just the value if not a tupple.
    """

    # Disable check if attribute exists on object, Microservice related are
    # not values on object
    _CHECK_ATTRIBUTE = False

    def __init__(self, source: str, microservice: PumpWoodMicroService,
                 model_class: str,  display_field: str = None, **kwargs):
        self.microservice = microservice
        self.model_class = model_class
        self.display_field = display_field
        self.source = source

        # Set as read only and not required, changes on foreign key must be
        # done using id
        kwargs['required'] = False
        kwargs['dump_only'] = True
        super(MicroserviceForeignKeyField, self).__init__(**kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        """Use microservice to get object at serialization."""
        self.microservice.login()
        object_pk = getattr(obj, self.source)

        # Return an empty object if object pk is None
        if object_pk is None:
            return {"model_class": self.model_class}

        object_data = self.microservice.list_one(
            model_class=self.model_class, pk=object_pk)
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

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "MicroserviceForeignKeyField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        return {
            'model_class': self.model_class, 'many': False,
            'display_field': self.display_field,
            'object_field': self.name}


class MicroserviceRelatedField(fields.Field):
    """
    Serializer field for related objects using microservice.

    It is an informational serializer to related models.
    """

    _CHECK_ATTRIBUTE = False

    def __init__(self, microservice: PumpWoodMicroService,
                 model_class: str,  foreign_key: str,
                 pk_field: str = 'id', order_by: str = ["id"],
                 help_text: str = "", read_only=False, **kwargs):
        self.microservice = microservice
        self.model_class = model_class
        self.foreign_key = foreign_key
        self.pk_field = pk_field
        self.order_by = order_by

        # Informational data for options end-point
        self.help_text = help_text
        self.read_only = read_only

        # Force field not be necessary for saving object
        kwargs['required'] = False
        kwargs['dump_only'] = True

        # Set as read only and not required, changes on foreign key must be
        # done using id
        super(MicroserviceRelatedField, self).__init__(**kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        """Use microservice to get object at serialization."""
        self.microservice.login()
        pk_field = getattr(obj, self.pk_field)
        return self.microservice.list_without_pag(
            model_class=self.model_class,
            filter_dict={self.foreign_key: pk_field},
            order_by=self.order_by)

    def _deserialize(self, value, attr, data, **kwargs):
        raise NotImplementedError(
            "MicroserviceRelatedField are read-only")

    def to_dict(self):
        """Return a dict with values to be used on options end-point."""
        return {
            'model_class': self.model_class, 'many': True,
            'pk_field': self.pk_field, 'order_by': self.order_by,
            'foreign_key': self.foreign_key}
