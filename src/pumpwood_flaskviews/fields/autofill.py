"""Marshmallow fields that auto-fill values from related objects."""
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
    """Cache hash components for AutoFill field lookups."""

    authorization_token: str | None
    """Request Authorization header value."""
    model_class: str
    """Target model class for the autofill lookup."""
    pk: str | int
    """Primary key (integer or composite Base64 string)."""
    field: str
    """Attribute on the related object used as the fill value."""
    apply_user_permission: bool
    """Whether row-permission filters apply to the lookup."""
    context: str = 'flaskviews--auto-fill-field'
    """Context identifier for the cache entry."""


class AutoFillFieldLocal(Field):
    """Auto-fill a field from a related local SQLAlchemy model.

    On deserialize, queries the related model and copies ``fill_field``
    into the current object.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    model_class: FlaskPumpWoodBaseModel | None = None
    """Resolved model class used to fetch autofill data."""

    def __init__(self, model_class: FlaskPumpWoodBaseModel | str,
                 source: str, fill_field: str,
                 complementary_source: dict[str, str] = {},
                 apply_user_permission: bool = False,
                 *args, **kwargs):
        """Initialize the local autofill field.

        Fetch information from a related object when saving the current
        instance.

        Args:
            model_class (FlaskPumpWoodBaseModel | str):
                Model class or import path for the related object.
            source (str):
                Field on the current object used as the foreign key.
            fill_field (str):
                Attribute on the related object copied into this field.
            complementary_source (dict[str, str]):
                Maps current-object fields to related-object pk fields
                for composite keys.
            apply_user_permission (bool):
                If True, uses default_query_get (row-permission filter).
            *args:
                Positional arguments forwarded to Marshmallow Field.
            **kwargs:
                Keyword arguments forwarded to Marshmallow Field.
        """
        # Set allow_none to True by default if not explicitly provided
        kwargs['allow_none'] = True
        kwargs['load_default'] = AUTO_FILL.value()
        self._pre_load_model_class = model_class
        self._source = source
        self._fill_field = fill_field
        self._complementary_source = complementary_source
        self._apply_user_permission = apply_user_permission
        super().__init__(*args, **kwargs)

    def _get_model_class(self) -> FlaskPumpWoodBaseModel:
        """Resolve model class at deserialization time.

        Returns:
            FlaskPumpWoodBaseModel:
                The related SQLAlchemy model class.
        """
        if self.model_class is None:
            self.model_class = _import_function_by_string(
                module=self._pre_load_model_class)
        return self.model_class

    @classmethod
    def validate_fields(cls, field_name: str, primary_keys: dict,
                        data: dict, related_model: str) -> None:
        """Ensure all pk fields required for autofill are present.

        Args:
            field_name (str):
                Name of the autofill field being deserialized.
            primary_keys (dict):
                Mapping of source fields to related-object pk fields.
            data (dict):
                Incoming payload for the object being saved.
            related_model (str):
                Related model name used in error messages.

        Raises:
            PumpWoodObjectSavingException:
                If any required primary-key field is missing from data.
        """
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

    def _get_related_primary_keys(self) -> dict[str, str]:
        """Build primary-key mapping for the related lookup.

        Returns:
            dict[str, str]:
                Source field names mapped to related-object pk fields.
        """
        primary_keys = {self._source: 'id'}
        primary_keys.update(self._complementary_source)
        return primary_keys

    def _build_fk(self, data: dict, primary_keys: dict
                  ) -> str | int:
        """Encode composite primary key from payload data.

        Args:
            data (dict):
                Incoming payload for the object being saved.
            primary_keys (dict):
                Mapping of source fields to related-object pk fields.

        Returns:
            str | int:
                Pumpwood primary key for the related object.
        """
        object_pk = CompositePkBase64Converter.dump(
            obj=data, primary_keys=primary_keys)
        return object_pk

    def _get_fill_value(self, data: dict, field_name: str) -> Any:
        """Fetch the fill value from the related local object.

        Args:
            data (dict):
                Incoming payload for the object being saved.
            field_name (str):
                Name of the autofill field being deserialized.

        Returns:
            Any:
                Value of ``fill_field`` on the related object.

        Raises:
            PumpWoodObjectDoesNotExist:
                If the related object was not found.
            PumpWoodOtherException:
                If ``fill_field`` is not defined on the related model.
        """
        model_class = self._get_model_class()
        primary_keys = self._get_related_primary_keys()

        # Validate if fields are correct
        self.validate_fields(
            field_name=field_name, primary_keys=primary_keys,
            data=data, related_model=model_class.__name__)

        # Build primary keys dictionary
        pk = self._build_fk(data=data, primary_keys=primary_keys)
        hash_dict = AutoFillFieldCacheHash(
            authorization_token=AuthFactory.get_auth_header()['Authorization'],
            model_class=model_class.__name__.lower(),
            pk=pk, field=self._fill_field,
            apply_user_permission=self._apply_user_permission)

        # Try to fetch data using cached information
        cached_data = PumpwoodFlaskGDiskCache.get(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        # Fetch information from database and treat the error if the object
        # was not found
        try:
            fill_object = None
            # Default query will validate if user has permission to
            # access the object
            if self._apply_user_permission:
                fill_object = model_class.default_query_get(pk=pk)
            else:
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
        """Resolve autofill value during object deserialization.

        Skips Marshmallow missing-value handling so AUTO_FILL defaults
        can run. Honors audit overwrite when present.

        Args:
            value:
                Raw input value (usually ignored).
            attr (str | None):
                Attribute name on the schema.
            data (dict | None):
                Full incoming payload.
            **kwargs:
                Marshmallow internal deserialization options.

        Returns:
            Any:
                Autofill value from the related object or audit override.

        Raises:
            PumpWoodObjectSavingException:
                If the source foreign-key field is missing from data.
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

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
    """Auto-fill a field from a related remote microservice model.

    On deserialize, retrieves ``fill_field`` from another service via
    ``PumpWoodMicroService.retrieve``.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    model_class: str
    """Remote model class name used to fetch autofill data."""

    def __init__(self, microservice: PumpWoodMicroService, model_class: str,
                 source: str, fill_field: str,
                 complementary_source: dict[str, str] = {},
                 apply_user_permission: bool = False,
                 *args, **kwargs):
        """Initialize the microservice autofill field.

        Fetch information from a remote object when saving the current
        instance.

        Args:
            microservice (PumpWoodMicroService):
                Client used to retrieve related object data.
            model_class (str):
                Remote model class name.
            source (str):
                Field on the current object used as the foreign key.
            fill_field (str):
                Key on the related object copied into this field.
            complementary_source (dict[str, str]):
                Maps current-object fields to related-object pk fields
                for composite keys.
            apply_user_permission (bool):
                If True, forwards request auth and skips logged-in
                microservice credentials.
            *args:
                Positional arguments forwarded to Marshmallow Field.
            **kwargs:
                Keyword arguments forwarded to Marshmallow Field.

        Raises:
            PumpWoodOtherException:
                If apply_user_permission is True while the microservice
                already has credentials set.
        """
        if microservice.is_credential_set() and apply_user_permission:
            msg = (
                "It is not possible to apply user permission to the "
                "microservice if the microservice is logged in. Please, "
                "use a non logged microservice (without credentials) or set "
                "apply_user_permission to False. Current microservice name: "
                "[{microservice}].")
            raise PumpWoodOtherException(
                msg, payload={"microservice": microservice.name})

        # Set allow_none to True by default if not explicitly provided
        kwargs['allow_none'] = True
        kwargs['load_default'] = AUTO_FILL.value()
        self.model_class = model_class
        self.microservice = microservice
        self._source = source
        self._fill_field = fill_field
        self._complementary_source = complementary_source
        self._apply_user_permission = apply_user_permission
        super().__init__(*args, **kwargs)

    @classmethod
    def validate_fields(cls, field_name: str, primary_keys: dict,
                        data: dict, related_model: str) -> None:
        """Ensure all pk fields required for autofill are present.

        Args:
            field_name (str):
                Name of the autofill field being deserialized.
            primary_keys (dict):
                Mapping of source fields to related-object pk fields.
            data (dict):
                Incoming payload for the object being saved.
            related_model (str):
                Related model name used in error messages.

        Raises:
            PumpWoodObjectSavingException:
                If any required primary-key field is missing from data.
        """
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

    def _get_related_primary_keys(self) -> dict[str, str]:
        """Build primary-key mapping for the related lookup.

        Returns:
            dict[str, str]:
                Source field names mapped to related-object pk fields.
        """
        primary_keys = {self._source: 'id'}
        primary_keys.update(self._complementary_source)
        return primary_keys

    def _build_fk(self, data: dict, primary_keys: dict
                  ) -> str | int:
        """Encode composite primary key from payload data.

        Args:
            data (dict):
                Incoming payload for the object being saved.
            primary_keys (dict):
                Mapping of source fields to related-object pk fields.

        Returns:
            str | int:
                Pumpwood primary key for the related object.
        """
        object_pk = CompositePkBase64Converter.dump(
            obj=data, primary_keys=primary_keys)
        return object_pk

    def _get_fill_value(self, data: dict, field_name: str) -> Any:
        """Fetch the fill value from the related microservice object.

        Args:
            data (dict):
                Incoming payload for the object being saved.
            field_name (str):
                Name of the autofill field being deserialized.

        Returns:
            Any:
                Value of ``fill_field`` on the related object.

        Raises:
            PumpWoodObjectDoesNotExist:
                If the related object was not found.
            PumpWoodOtherException:
                If ``fill_field`` is missing from the retrieve response.
        """
        primary_keys = self._get_related_primary_keys()

        # Validate if fields are correct
        self.validate_fields(
            field_name=field_name, primary_keys=primary_keys,
            data=data, related_model=self.model_class)

        # Build primary keys dictionary
        pk = self._build_fk(data=data, primary_keys=primary_keys)
        hash_dict = AutoFillFieldCacheHash(
            authorization_token=AuthFactory.get_auth_header()['Authorization'],
            model_class=self.model_class.lower(),
            pk=pk, field=self._fill_field,
            apply_user_permission=self._apply_user_permission)

        # Try to fetch data using cached information
        cached_data = PumpwoodFlaskGDiskCache.get(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        # Fetch information from database and treat the error if the object
        # was not found
        try:
            fill_data = None
            if self._apply_user_permission:
                # To apply user permission on the request, the auth header
                # is fetched from the current user request.
                # The microservice object must not be logged in other
                # to allow the auth header injection.
                auth_header = AuthFactory.get_auth_header()
                fill_data = self.microservice.retrieve(
                    model_class=self.model_class, pk=pk,
                    fields=[self._fill_field],
                    auth_header=auth_header)
            else:
                # Use the logged microservice object to retrieve data,
                # usually this is a super user associated microservice.
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
        """Resolve autofill value during object deserialization.

        Skips Marshmallow missing-value handling so AUTO_FILL defaults
        can run. Honors audit overwrite when present.

        Args:
            value:
                Raw input value (usually ignored).
            attr (str | None):
                Attribute name on the schema.
            data (dict | None):
                Full incoming payload.
            **kwargs:
                Marshmallow internal deserialization options.

        Returns:
            Any:
                Autofill value from the related object or audit override.

        Raises:
            PumpWoodObjectSavingException:
                If the source foreign-key field is missing from data.
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        if self._source not in data.keys():
            msg = (
                "It is not possible to get key [{source}] to "
                "request autofill data at model [{model}]")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "source": self._source,
                    "model": self.model_class})

        fill_value = self._get_fill_value(
            data=data, field_name=attr)
        return fill_value
