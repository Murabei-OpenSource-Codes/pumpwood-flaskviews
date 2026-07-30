"""Orchestrate bulk-save column filling for Pumpwood data views."""
import pandas as pd
from typing import Any
from dataclasses import dataclass
from pumpwood_communication.type import (
    PumpwoodDataclassMixin, BulkSaveMicroserviceAutoFillField,
    BulkSaveLocalAutoFillField, BulkSaveDefaultField,
    MixinBulkSaveField)
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_communication.exceptions import (
    PumpWoodOtherException, PumpWoodDataLoadingException)
from pumpwood_flaskviews.cache import PumpwoodFlaskGDiskCache

BulkSaveField = (
    str
    | BulkSaveLocalAutoFillField
    | BulkSaveMicroserviceAutoFillField
    | BulkSaveDefaultField
)
"""Column name or bulk-save field definition for expected_cols_bulk_save."""


@dataclass
class BulkSaveAutoFillFieldCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for auto fill on bulk save."""

    model_class: str
    """Model class for the autofill field."""
    pk: str | int
    """Pk associated with object to get the autofill field data."""
    field: str
    """Field to extract data to fill object."""
    context: str = 'flaskviews--bulk-save-auto-fill'
    """Content of the file that will be returned at the action."""


class FillBulkSaveFields:
    """Fill bulk save fields."""

    @classmethod
    def run(
            cls, data: pd.DataFrame,
            fields: list[BulkSaveField],
            microservice: PumpWoodMicroService) -> pd.DataFrame:
        """Fill bulk-save columns and validate the final payload shape.

        Args:
            data (pd.DataFrame):
                Bulk save data to fill the fields.
            fields (list[BulkSaveField]):
                View `expected_cols_bulk_save` entries: plain column
                names, autofill fields, or default-value fields.
            microservice (PumpWoodMicroService):
                Microservice used for remote autofill lookups.

        Returns:
            pd.DataFrame:
                Dataframe with filled columns restricted to the
                expected bulk-save schema.

        Raises:
            PumpWoodDataLoadingException:
                If autofill configuration or final columns are invalid.
            PumpWoodOtherException:
                If `expected_cols_bulk_save` contains duplicate columns.
        """
        for field in fields:
            if isinstance(field, BulkSaveMicroserviceAutoFillField):
                data = data.pipe(
                    cls.fill_auto_microservice, field=field,
                    microservice=microservice)
            elif isinstance(field, BulkSaveLocalAutoFillField):
                data = data.pipe(cls.fill_auto_local, field=field)
            elif isinstance(field, BulkSaveDefaultField):
                data = data.pipe(cls.fill_default, field=field)
        return cls.validate_data(data=data, fields=fields)

    @classmethod
    def get_field_cache(
            cls, model_class: str, pk: str | int, field: str) -> Any:
        """Get a cached autofill value for one related object.

        Args:
            model_class (str):
                Model class of the object used to fill values.
            pk (str | int):
                Primary key of the related object.
            field (str):
                Attribute name to read from the cached object.

        Returns:
            Any:
                Cached value, or None when cache is empty.
        """
        hash_dict = BulkSaveAutoFillFieldCacheHash(
            model_class=model_class, pk=pk, field=field)
        return PumpwoodFlaskGDiskCache.get(hash_dict)

    @classmethod
    def set_field_cache(
            cls, model_class: str, pk: str | int, field: str,
            value: Any) -> Any:
        """Set a cached autofill value for one related object.

        Args:
            model_class (str):
                Model class of the object used to fill values.
            pk (str | int):
                Primary key of the related object.
            field (str):
                Attribute name stored in the cache entry.
            value (Any):
                Value to store for later autofill lookups.

        Returns:
            Any:
                Value returned by the cache backend after storing.
        """
        hash_dict = BulkSaveAutoFillFieldCacheHash(
            model_class=model_class, pk=pk, field=field)
        return PumpwoodFlaskGDiskCache.set(
            hash_dict=hash_dict, value=value)

    @classmethod
    def _validate_object_fk_column(
            cls, data: pd.DataFrame, field: MixinBulkSaveField) -> None:
        """Validate autofill foreign key column on bulk save payload.

        Args:
            data (pd.DataFrame):
                Bulk save data to fill the fields.
            field (MixinBulkSaveField):
                Autofill field definition from the view.

        Raises:
            PumpWoodDataLoadingException:
                If object_fk_column is missing or not present on data.
        """
        object_fk_column = getattr(field, 'object_fk_column', None)
        if object_fk_column is None:
            msg = (
                "Bulk save autofill for field [{field_name}] requires "
                "'object_fk_column', but it is not configured. Check "
                "'expected_cols_bulk_save' on the view.")
            raise PumpWoodDataLoadingException(
                msg.format(field_name=field.field),
                payload={
                    "field_name": field.field,
                    "object_fk_column": object_fk_column})

        if object_fk_column not in data.columns:
            same_as_target = object_fk_column == field.field
            misconfig_hint = (
                " 'object_fk_column' matches the autofill target field; "
                "it should reference a foreign key column present in the "
                "bulk save payload."
                if same_as_target else "")
            msg = (
                "Bulk save autofill for field [{field_name}] requires "
                "foreign key column [{object_fk_column}] on the payload, "
                "but it is missing from bulk save data.{misconfig_hint} "
                "Check 'expected_cols_bulk_save' and include the FK "
                "column in the payload or fix 'object_fk_column'.")
            raise PumpWoodDataLoadingException(
                msg.format(
                    field_name=field.field,
                    object_fk_column=object_fk_column,
                    misconfig_hint=misconfig_hint),
                payload={
                    "field_name": field.field,
                    "object_fk_column": object_fk_column,
                    "data_columns": list(data.columns),
                    "same_as_target_field": same_as_target})

    @classmethod
    def fill_auto_local(cls, data: pd.DataFrame,
                        field: BulkSaveLocalAutoFillField) -> pd.DataFrame:
        """Fill one column from a local related model on bulk save.

        Args:
            data (pd.DataFrame):
                Bulk save payload before the target column is filled.
            field (BulkSaveLocalAutoFillField):
                Local autofill definition from the view.

        Returns:
            pd.DataFrame:
                Input dataframe with `field.field` populated from the
                related model.

        Raises:
            PumpWoodDataLoadingException:
                If `object_fk_column` is missing from the payload or
                related keys cannot be resolved.
            PumpWoodOtherException:
                If the related model does not expose `fill_col`.
        """
        cls._validate_object_fk_column(data=data, field=field)
        unique_fk_columns = data[field.object_fk_column].unique().tolist()
        map_fk_fill_data = {}
        missing_cache = []

        # Get data from localcache if avaiable and allowed
        for fk_pk in unique_fk_columns:
            if not field.use_cache:
                missing_cache.append(fk_pk)
            else:
                cached_value = cls.get_field_cache(
                    model_class=field.cls_fill_model_class.__name__,
                    pk=fk_pk, field=field.fill_col)
                if cached_value is None:
                    missing_cache.append(int(fk_pk))
                else:
                    map_fk_fill_data[fk_pk] = cached_value

        # Fetch data from database
        if len(missing_cache) != 0:
            fk_objects = field.cls_fill_model_class.query_list(
                filter_dict={"id__in": missing_cache})
            for fk_obj in fk_objects:
                if not hasattr(fk_obj, field.fill_col):
                    msg = (
                        "Foreign object [{model_class}] used to fill the "
                        "value does not have the expected field [{fill_col}]."
                    ).format(
                        fill_col=field.fill_col,
                        model_class=field.cls_fill_model_class.__name__)
                    raise PumpWoodOtherException(msg)

                fk_fill_value = getattr(fk_obj, field.fill_col)
                map_fk_fill_data[fk_obj.id] = fk_fill_value

                # Set cache
                cls.set_field_cache(
                    model_class=field.cls_fill_model_class.__name__,
                    pk=fk_pk, field=field.fill_col, value=fk_fill_value)

        # Validate if all foreign keys are set on the map dictonary
        cls.validate_fks(
            unique_fk_columns=unique_fk_columns,
            map_fk_fill_data=map_fk_fill_data,
            related_model_name=field.cls_fill_model_class.__name__,
            field_name=field.field)

        data[field.field] = \
            data[field.object_fk_column].map(map_fk_fill_data)
        return data

    @classmethod
    def fill_auto_microservice(cls, data: pd.DataFrame,
                               field: BulkSaveMicroserviceAutoFillField,
                               microservice: PumpWoodMicroService
                               ) -> pd.DataFrame:
        """Fill one column from a remote model on bulk save.

        Args:
            data (pd.DataFrame):
                Bulk save payload before the target column is filled.
            field (BulkSaveMicroserviceAutoFillField):
                Remote autofill definition from the view.
            microservice (PumpWoodMicroService):
                Microservice used to fetch related object values.

        Returns:
            pd.DataFrame:
                Input dataframe with `field.field` populated from the
                related model.

        Raises:
            PumpWoodDataLoadingException:
                If `object_fk_column` is missing from the payload or
                related keys cannot be resolved.
        """
        cls._validate_object_fk_column(data=data, field=field)
        unique_fk_columns = data[field.object_fk_column].unique().tolist()
        map_fk_fill_data = {}
        missing_cache = []

        # Get data from localcache if avaiable and allowed
        for fk_pk in unique_fk_columns:
            if not field.use_cache:
                missing_cache.append(fk_pk)
            else:
                cached_value = cls.get_field_cache(
                    model_class=field.fill_model_class,
                    pk=fk_pk, field=field.fill_col)
                if cached_value is None:
                    missing_cache.append(fk_pk)
                else:
                    map_fk_fill_data[fk_pk] = cached_value

        # Fetch data from database
        if len(missing_cache) != 0:
            fk_objects = microservice.list_without_pag(
                model_class=field.fill_model_class,
                filter_dict={"id__in": missing_cache},
                fields=['id', field.fill_col])
            for fk_obj in fk_objects:
                fk_fill_value = fk_obj[field.fill_col]
                map_fk_fill_data[fk_obj['id']] = fk_fill_value

                # Set cache
                cls.set_field_cache(
                    model_class=field.fill_model_class,
                    pk=fk_pk, field=field.fill_col, value=fk_fill_value)

        # Validate if all foreign keys are set on the map dictonary
        cls.validate_fks(
            unique_fk_columns=unique_fk_columns,
            map_fk_fill_data=map_fk_fill_data,
            related_model_name=field.fill_model_class,
            field_name=field.field)

        data[field.field] = \
            data[field.object_fk_column].map(map_fk_fill_data)
        return data

    @classmethod
    def fill_default(cls, data: pd.DataFrame, field: BulkSaveDefaultField
                    ) -> pd.DataFrame:
        """Apply a default value to one bulk-save column.

        Args:
            data (pd.DataFrame):
                Bulk save payload before defaults are applied.
            field (BulkSaveDefaultField):
                Default-value definition from the view.

        Returns:
            pd.DataFrame:
                Input dataframe with missing or null values filled.
        """
        if field.field not in data.columns:
            data[field.field] = field.default
        else:
            data[field.field] = data[field.field].fillna(field.default)
        return data

    @classmethod
    def _bulk_save_fields_for_payload(
            cls, fields: list[BulkSaveField]) -> list:
        """Serialize bulk save field definitions for exception payloads.

        Args:
            fields (list[BulkSaveField]):
                Bulk save field definitions from the view.

        Returns:
            list:
                JSON-serializable field definitions.
        """
        payload_fields = []
        for item in fields:
            if isinstance(item, MixinBulkSaveField):
                field_data = item.to_dict()
                fill_model = field_data.get('fill_model_class')
                if isinstance(fill_model, type):
                    field_data['fill_model_class'] = fill_model.__name__
                payload_fields.append(field_data)
            else:
                payload_fields.append(item)
        return payload_fields

    @classmethod
    def validate_fks(cls, unique_fk_columns: list, map_fk_fill_data: dict,
                     field_name: str, related_model_name: str,
                     raise_error: bool = True) -> None:
        """Validate autofill foreign-key coverage for one column.

        Args:
            unique_fk_columns (list):
                Distinct foreign-key values present on the payload.
            map_fk_fill_data (dict):
                Mapping from foreign-key value to fill value.
            field_name (str):
                Target bulk-save column being filled.
            related_model_name (str):
                Related model queried for autofill values.
            raise_error (bool):
                When False, skip raising on missing related keys.

        Raises:
            PumpWoodDataLoadingException:
                If related keys from the payload were not found.
        """
        set_of_map_fks = set(list(map_fk_fill_data.keys()))
        set_unique_fk_columns = set(unique_fk_columns)
        if (set_of_map_fks != set_unique_fk_columns) and raise_error:
            missing_keys = set_unique_fk_columns - set_of_map_fks
            list_set_unique_fk_columns = list(set_unique_fk_columns)
            list_set_of_map_fks = list(set_of_map_fks)
            list_missing_keys = list(missing_keys)
            msg = (
                "The foreign key map used to fill values for field " +
                "[{field_name}] on bulk save was not able to fetch some " +
                "keys on related table[{related_model_name}]. Keys " +
                "information truncated at 20:\n" +
                "- Keys on data: {set_unique_fk_columns_msg}\n" +
                "- Keys found on related model: {set_of_map_fks_msg}\n"
                "- Missing keys: {missing_keys_msg}")
            raise PumpWoodDataLoadingException(
                msg, payload={
                    "field_name": field_name,
                    "related_model_name": related_model_name,
                    "set_unique_fk_columns_msg": list_set_unique_fk_columns[:20], # NOQA
                    "set_of_map_fks_msg": list_set_of_map_fks[:20],
                    "missing_keys_msg": list_missing_keys[:20],
                    "set_unique_fk_columns": list_set_unique_fk_columns,
                    "set_of_map_fks": list_set_of_map_fks,
                    "missing_keys": list_missing_keys})
        return None

    @classmethod
    def validate_data(
            cls, data: pd.DataFrame,
            fields: list[BulkSaveField]) -> pd.DataFrame:
        """Validate and restrict bulk-save data to expected columns.

        Args:
            data (pd.DataFrame):
                Filled bulk-save dataframe.
            fields (list[BulkSaveField]):
                View `expected_cols_bulk_save` definitions.

        Returns:
            pd.DataFrame:
                Input dataframe restricted to expected columns.

        Raises:
            PumpWoodOtherException:
                If duplicate expected column names are configured.
            PumpWoodDataLoadingException:
                If required columns are missing after filling.
        """
        final_cols: list[str] = []
        for x in fields:
            if isinstance(x, MixinBulkSaveField):
                final_cols.append(x.field)
            else:
                final_cols.append(x)

        has_duplicates = len(final_cols) != len(set(final_cols))
        if has_duplicates:
            msg = (
                "There are duplicates in 'expected_cols_bulk_save' "
                "attribute at view, check implementation and correct it. "
                "Actual values [{expected_cols_bulk_save}]")
            raise PumpWoodOtherException(
                msg, payload={
                    "expected_cols_bulk_save": cls._bulk_save_fields_for_payload(  # NOQA
                        fields=fields),
                    "expected_column_names": final_cols})

        data_columns = set(data.columns)
        missing_cols = set(final_cols) - data_columns
        if len(missing_cols):
            msg = (
                "Some of the necessary columns are not at the buck save "
                "{missing_cols} data. Data columns {data_columns}; "
                "expected columns [{expected_columns}]")
            raise PumpWoodDataLoadingException(
                msg, payload={
                    "missing_cols": list(missing_cols),
                    "data_columns": list(data_columns),
                    "expected_columns": list(final_cols)
                })
        return data.loc[:, final_cols]
