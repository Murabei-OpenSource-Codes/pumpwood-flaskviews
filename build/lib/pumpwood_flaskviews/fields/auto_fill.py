"""Fields to auto fill usign other objects data."""
from typing import Any
from dataclasses import dataclass
from pumpwood_communication.type import PumpwoodDataclassMixin
from pumpwood_flaskviews.model import FlaskPumpWoodBaseModel
from marshmallow import missing
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.fields.aux import (
    _get_overwrite_audit, _import_function_by_string)
from pumpwood_flaskviews.cache import PumpwoodFlaskGDiskCache


@dataclass
class AutoFillFieldLocalCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for AutoFillFieldLocal."""

    model_class: str
    """Model class for the autofill field."""
    pk: str | int
    """Pk associated with objecto to get the autofill field data."""
    field: str
    """Field to extract data to fill object."""
    context: str = 'flaskviews--auto-fill-field'
    """Content of the file that will be returned at the action."""


class AutoFillFieldLocal:
    """Define a row permission field that is auto filed using other model.

    It will query a local model and fill the value with row_permission_id
    from the other model.

    I will also keep the validation of user allowed to use the
    row_permission_id.
    """

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

    def _get_fill_value(self, pk: str | int, field: str) -> Any:
        """Get fill value from fill object."""
        model_class = self._get_model_class()
        cache_hash = AutoFillFieldLocalCacheHash(
            model_class=model_class, pk=pk,
            field=field)


    def _deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        # Fetch row_permission_id from fill model


        value_temp = super()._deserialize(value, attr, data, **kwargs)
        self._validate_user_access(value=value_temp, attr=attr)
        return value_temp
