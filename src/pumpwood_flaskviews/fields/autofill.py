"""Fields to auto fill usign other objects data."""
from typing import Any
from marshmallow.fields import Field
from marshmallow import missing
from dataclasses import dataclass
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_communication.type import (
    PumpwoodDataclassMixin, AUTO_FILL)
from pumpwood_communication.exceptions import (
    PumpWoodOtherException, PumpWoodObjectSavingException,
    PumpWoodObjectDoesNotExist)
from pumpwood_communication.serializers import CompositePkBase64Converter
from pumpwood_flaskviews.model import FlaskPumpWoodBaseModel
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.fields.aux import (
    _get_overwrite_audit, _import_function_by_string)
from pumpwood_flaskviews.cache import PumpwoodFlaskGDiskCache


@dataclass
class AutoFillFieldCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for AutoFillFieldLocal."""

    model_class: str
    """Model class for the autofill field."""
    pk: str | int
    """Base64 dictionary or interger for object id."""
    field: str
    """Field to extract data to fill object."""
    context: str = 'flaskviews--auto-fill-field'
    """Content of the file that will be returned at the action."""


class AutoFillFieldLocal(Field):
    """Define a row permission field that is auto filed using other model.

    It will query a local model using a sqlalchemy call and fill the
    value with `fill_field` attribute from the other model.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    model_class: FlaskPumpWoodBaseModel = None
    """Model class that will be loded to request autofill field."""

    def __init__(self, model_class: FlaskPumpWoodBaseModel | str,
                 source: str, fill_field: str,
                 complementary_source: dict[str, str] = {},
                 *args, **kwargs):
        """__init__.

        Fetch information from other object to fill the actual on
        saving.

        Args:
            model_class (FlaskPumpWoodBaseModel | str):
                A string for path to import the model class or the class
                of the object from where it will be retrived the model
                class.
            source (str):
                Field at the actual object that will be considered a foreign
                key to fetch inforamtion from other.
            fill_field (str):
                Column at the 'fill object' that will be used to
                fill the 'action object' field.
            complementary_source (Dict[str, str]):
                Complementary foreignkey, the dictonary will map the
                information from the actual object -> destiny object.
            *args:
                Other posicional arguments used at marshmallow fields.
            **kwargs:
                Other named arguments used at marshmallow fields.
        """
        # Set allow_none to True by default if not explicitly provided
        kwargs['allow_none'] = True
        kwargs['load_default'] = AUTO_FILL.value()
        self._pre_load_model_class = model_class
        self._source = source
        self._fill_field = fill_field
        self._complementary_source = complementary_source
        super().__init__(*args, **kwargs)

    def _get_model_class(self) -> FlaskPumpWoodBaseModel:
        """Load model class at serialization to avoid circular dependency."""
        if self.model_class is None:
            self.model_class = _import_function_by_string(
                module=self._pre_load_model_class)
        return self.model_class

    @classmethod
    def validate_fields(cls, field_name: str, primary_keys: dict,
                        data: dict, related_model: str) -> bool:
        """Validate fields to check for all fields necessary to autofill."""
        set_primary_keys_keys = set(primary_keys.keys())
        set_data_keys = set(data.keys())
        missing_keys = set_primary_keys_keys - set_data_keys
        if len(missing_keys) != 0:
            msg = (
                "Autofill field [{field_name}] use fields [{primary_keys}] "
                "to query related object and [{related_model}] are not "
                "present on object data.")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "field_name": field_name,
                    "primary_keys": set_primary_keys_keys,
                    "related_model": related_model})

    def _get_related_primary_keys(self):
        """Get related primary keys fields and values."""
        primary_keys = {self._source: 'id'}
        primary_keys.update(self._complementary_source)
        return primary_keys

    def _build_fk(self, data: dict, primary_keys: dict
                  ) -> FlaskPumpWoodBaseModel:
        """Build fk dictionary using the object information."""
        object_pk = CompositePkBase64Converter.dump(
            obj=data, primary_keys=primary_keys)
        return object_pk

    def _get_fill_value(self, data: dict, field_name: str) -> Any:
        """Get fill value from fill object."""
        model_class = self._get_model_class()
        primary_keys = self._get_related_primary_keys()

        # Validate if fields are correct
        self.validate_fields(
            field_name=field_name, primary_keys=primary_keys,
            data=data, related_model=model_class.__name__)

        # Build primary keys dictionary
        pk = self._build_fk(data=data, primary_keys=primary_keys)
        hash_dict = AutoFillFieldCacheHash(
            model_class=model_class.__name__.lower(),
            pk=pk, field=self._fill_field)

        # Try to fetch data using cached information
        cached_data = PumpwoodFlaskGDiskCache.get(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        # Fetch information from database and treat the error if the object
        # was not found
        try:
            fill_object = model_class.query_get(pk=pk)
        except PumpWoodObjectDoesNotExist as e:
            msg = (
                "Local Autofill was not able to fetch information from " +
                "to local the attribute [{attribute}] at model [{model}] " +
                ". The object pk[{pk}] was not found.")
            raise PumpWoodObjectDoesNotExist(
                msg, payload={
                    "model": model_class.__name__,
                    "attribute": self._fill_field,
                    "pk": pk,
                    "not_found_payload": e.to_dict()})

        if not hasattr(fill_object, self._fill_field):
            msg = (
                "Local Autofill field is not correctly configured, "
                "it is not possible to local the attribute [{attribute}] "
                "at model [{model}]")
            raise PumpWoodOtherException(
                msg, payload={
                    "attribute": self._fill_field,
                    "model": model_class.__name__})

        fill_value = getattr(fill_object, self._fill_field)
        PumpwoodFlaskGDiskCache.set(hash_dict=hash_dict, value=fill_value)
        return fill_value

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        # Fetch row_permission_id from fill model
        if self._source not in data.keys():
            model_class = self._get_model_class()
            msg = (
                "It is not possible to get key [{source}] to "
                "request autofill data at model [{model}]")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "source": self._source,
                    "model": model_class.__name__})

        fill_value = self._get_fill_value(
            data=data, field_name=attr)
        return fill_value


class AutoFillFieldMicroservice(Field):
    """Define a row permission field that is auto filed using other model.

    It will query a non-local model using a microservice call and fill the
    value with `fill_field` key from the other model.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    model_class: FlaskPumpWoodBaseModel
    """Model class that will be loded to request autofill field."""

    def __init__(self, microservice: PumpWoodMicroService, model_class: str,
                 source: str, fill_field: str,
                 complementary_source: dict[str, str] = {},
                 *args, **kwargs):
        """__init__.

        Fetch information from other object to fill the actual on
        saving.

        Args:
            model_class (str):
                String defining the model class.
            microservice (PumpWoodMicroService):
                Microservice object to request autofill data from other
                services.
            source (str):
                Field at the actual object that will be considered a foreign
                key to fetch inforamtion from other.
            fill_field (str):
                Column at the 'fill object' that will be used to
                fill the 'action object' field.
            complementary_source (Dict[str, str]):
                Complementary foreignkey, the dictonary will map the
                information from the actual object -> destiny object.
            *args:
                Other posicional arguments used at marshmallow fields.
            **kwargs:
                Other named arguments used at marshmallow fields.
        """
        # Set allow_none to True by default if not explicitly provided
        kwargs['allow_none'] = True
        kwargs['load_default'] = AUTO_FILL.value()
        self.model_class = model_class
        self.microservice = microservice
        self._source = source
        self._fill_field = fill_field
        self._complementary_source = complementary_source
        super().__init__(*args, **kwargs)

    @classmethod
    def validate_fields(cls, field_name: str, primary_keys: dict,
                        data: dict, related_model: str) -> bool:
        """Validate fields to check for all fields necessary to autofill."""
        set_primary_keys_keys = set(primary_keys.keys())
        set_data_keys = set(data.keys())
        missing_keys = set_primary_keys_keys - set_data_keys
        if len(missing_keys) != 0:
            msg = (
                "Microservice Autofill field [{field_name}] use fields "
                "[{primary_keys}] to query related object and "
                "[{related_model}] are not present on object data.")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "field_name": field_name,
                    "primary_keys": set_primary_keys_keys,
                    "related_model": related_model})

    def _get_related_primary_keys(self):
        """Get related primary keys fields and values."""
        primary_keys = {self._source: 'id'}
        primary_keys.update(self._complementary_source)
        return primary_keys

    def _build_fk(self, data: dict, primary_keys: dict
                  ) -> FlaskPumpWoodBaseModel:
        """Build fk dictionary using the object information."""
        object_pk = CompositePkBase64Converter.dump(
            obj=data, primary_keys=primary_keys)
        return object_pk

    def _get_fill_value(self, data: dict, field_name: str) -> Any:
        """Get fill value from fill object."""
        primary_keys = self._get_related_primary_keys()

        # Validate if fields are correct
        self.validate_fields(
            field_name=field_name, primary_keys=primary_keys,
            data=data, related_model=self.model_class)

        # Build primary keys dictionary
        pk = self._build_fk(data=data, primary_keys=primary_keys)
        hash_dict = AutoFillFieldCacheHash(
            model_class=self.model_class.lower(),
            pk=pk, field=self._fill_field)

        # Try to fetch data using cached information
        cached_data = PumpwoodFlaskGDiskCache.get(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        # Fetch information from database and treat the error if the object
        # was not found
        try:
            fill_data = self.microservice.retrieve(
                model_class=self.model_class, pk=pk,
                fields=[self._fill_field])
        except PumpWoodObjectDoesNotExist as e:
            msg = (
                "Local Autofill was not able to fetch information from " +
                "to local the attribute [{attribute}] at model [{model}] " +
                ". The object pk[{pk}] was not found.")
            raise PumpWoodObjectDoesNotExist(
                msg, payload={
                    "model": self.model_class,
                    "attribute": self._fill_field,
                    "pk": pk,
                    "not_found_payload": e.to_dict()})

        if self._fill_field not in fill_data.keys():
            msg = (
                "Microservice Autofill field is not correctly configured, "
                "it is not possible to locate the key [{attribute}] "
                "at model [{model}] data")
            raise PumpWoodOtherException(
                msg, payload={
                    "attribute": self._fill_field,
                    "model": self.model_class})

        fill_value = fill_data.get(self._fill_field)
        PumpwoodFlaskGDiskCache.set(hash_dict=hash_dict, value=fill_value)
        return fill_value

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        # Fetch row_permission_id from fill model
        if self._source not in data.keys():
            msg = (
                "It is not possible to get key [{source}] to "
                "request autofill data at model [{model}]")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "source": self._source,
                    "model": self.model_class.__name__})

        fill_value = self._get_fill_value(
            data=data, field_name=attr)
        return fill_value
