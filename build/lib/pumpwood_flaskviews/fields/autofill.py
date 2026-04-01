"""Fields to auto fill usign other objects data."""
from typing import Any
from marshmallow.fields import Field
from marshmallow import missing
from dataclasses import dataclass
from pumpwood_communication.type import PumpwoodDataclassMixin
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_communication.exceptions import (
    PumpWoodOtherException, PumpWoodObjectSavingException)
from pumpwood_flaskviews.model import FlaskPumpWoodBaseModel
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.fields.aux import (
    _get_overwrite_audit, _import_function_by_string)
from pumpwood_flaskviews.cache import PumpwoodFlaskGDiskCache
from pumpwood_communication.type import AUTO_FILL


@dataclass
class AutoFillFieldCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for AutoFillFieldLocal."""

    model_class: str
    """Model class for the autofill field."""
    pk: str | int
    """Pk associated with objecto to get the autofill field data."""
    field: str
    """Field to extract data to fill object."""
    context: str = 'flaskviews--auto-fill-field'
    """Content of the file that will be returned at the action."""


class AutoFillFieldLocal(Field):
    """Define a row permission field that is auto filed using other model.

    It will query a local model using a sqlalchemy call and fill the
    value with `fill_col` attribute from the other model.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    model_class: FlaskPumpWoodBaseModel = None
    """Model class that will be loded to request autofill field."""

    def __init__(self, fill_model_class: FlaskPumpWoodBaseModel | str,
                 object_fk_column: str, fill_col: str,
                 *args, **kwargs):
        """__init__.

        Fetch information from other object to fill the actual on
        saving.

        Args:
            fill_model_class (FlaskPumpWoodBaseModel | str):
                A string for path to import the model class or the class
                of the object from where it will be retrived the model
                class.
            object_fk_column (str):
                Field at the actual object that will be considered a foreign
                key to fetch inforamtion from other.
            fill_col (str):
                Column at the 'fill object' that will be used to
                fill the 'action object' field.
            *args:
                Other posicional arguments used at marshmallow fields.
            **kwargs:
                Other named arguments used at marshmallow fields.
        """
        # Set allow_none to True by default if not explicitly provided
        kwargs['allow_none'] = True
        kwargs['load_default'] = AUTO_FILL.value()
        self._pre_load_model_class = fill_model_class
        self._object_fk_column = object_fk_column
        self._fill_col = fill_col
        super().__init__(*args, **kwargs)

    def _get_model_class(self) -> FlaskPumpWoodBaseModel:
        """Load model class at serialization to avoid circular dependency."""
        if self.model_class is None:
            self.model_class = _import_function_by_string(
                module=self._pre_load_model_class)
        return self.model_class

    def _get_fill_value(self, pk: str) -> Any:
        """Get fill value from fill object."""
        model_class = self._get_model_class()
        hash_dict = AutoFillFieldCacheHash(
            model_class=model_class.__name__.lower(),
            pk=pk, field=self._fill_col)

        # Try to fetch data using cached information
        cached_data = PumpwoodFlaskGDiskCache.get(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        # Fetch information from database
        fill_object = model_class.query_get(pk=pk)
        if not hasattr(fill_object, self._fill_col):
            msg = (
                "Local Autofill field is not correctly configured, "
                "it is not possible to local the attribute [{attribute}] "
                "at model [{model}]")
            raise PumpWoodOtherException(
                msg, payload={
                    "attribute": self._fill_col,
                    "model": model_class.__name__})

        fill_value = getattr(fill_object, self._fill_col)
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
        if self._object_fk_column not in data.keys():
            model_class = self._get_model_class()
            msg = (
                "It is not possible to get key [{object_fk_column}] to "
                "request autofill data at model [{model}]")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "object_fk_column": self._object_fk_column,
                    "model": model_class.__name__})
        object_fk = data.get(self._object_fk_column)
        fill_value = self._get_fill_value(pk=object_fk)
        return fill_value


class AutoFillFieldMicroservice(Field):
    """Define a row permission field that is auto filed using other model.

    It will query a non-local model using a microservice call and fill the
    value with `fill_col` key from the other model.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    model_class: FlaskPumpWoodBaseModel
    """Model class that will be loded to request autofill field."""

    def __init__(self, microservice: PumpWoodMicroService, model_class: str,
                 object_fk_column: str, fill_col: str, *args, **kwargs):
        """__init__.

        Fetch information from other object to fill the actual on
        saving.

        Args:
            model_class (str):
                String defining the model class.
            microservice (PumpWoodMicroService):
                Microservice object to request autofill data from other
                services.
            object_fk_column (str):
                Field at the actual object that will be considered a foreign
                key to fetch inforamtion from other.
            fill_col (str):
                Column at the 'fill object' that will be used to
                fill the 'action object' field.
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
        self._object_fk_column = object_fk_column
        self._fill_col = fill_col
        super().__init__(*args, **kwargs)

    def _get_fill_value(self, pk: str) -> Any:
        """Get fill value from fill object."""
        hash_dict = AutoFillFieldCacheHash(
            model_class=self.model_class.lower(),
            pk=pk, field=self._fill_col)

        # Try to fetch data using cached information
        cached_data = PumpwoodFlaskGDiskCache.get(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        fill_data = self.microservice.retrieve(
            model_class=self.model_class, pk=pk,
            fields=[self._fill_col])
        if self._fill_col not in fill_data.keys():
            msg = (
                "Microservice Autofill field is not correctly configured, "
                "it is not possible to locate the key [{attribute}] "
                "at model [{model}] data")
            raise PumpWoodOtherException(
                msg, payload={
                    "attribute": self._fill_col,
                    "model": self.model_class})

        fill_value = fill_data.get(self._fill_col)
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
        if self._object_fk_column not in data.keys():
            msg = (
                "It is not possible to get key [{object_fk_column}] to "
                "request autofill data at model [{model}]")
            raise PumpWoodObjectSavingException(
                msg, payload={
                    "object_fk_column": self._object_fk_column,
                    "model": self.model_class.__name__})

        object_fk = data.get(self._object_fk_column)
        fill_value = self._get_fill_value(pk=object_fk)
        return fill_value
