"""Module with auxiliary functions for data views on Pumpwood."""
import pandas as pd
from typing import Any
from dataclasses import dataclass
from pumpwood_communication.type import (
    PumpwoodDataclassMixin, BulkSaveMicroserviceAutoFillField,
    BulkSaveLocalAutoFillField)
from pumpwood_communication.microservices import PumpWoodMicroService
from pumpwood_communication.exceptions import PumpWoodOtherException
from pumpwood_flaskviews.cache import PumpwoodFlaskGDiskCache


@dataclass
class BulkSaveAutoFillFieldCacheHash(PumpwoodDataclassMixin):
    """Dictionary to create cache hash dict for auto fill on bulk save."""

    model_class: str
    """Model class for the autofill field."""
    pk: str | int
    """Pk associated with objecto to get the autofill field data."""
    field: str
    """Field to extract data to fill object."""
    context: str = 'flaskviews--bulk-save-auto-fill'
    """Content of the file that will be returned at the action."""


class FillBulkSaveFields:
    """Fill bulk save fields."""

    @classmethod
    def run(cls, data: pd.DataFrame, fields: list,
            microservice: PumpWoodMicroService) -> pd.DataFrame:
        """Fill the auto fill columns on bulk save.

        Args:
            data (pd.DataFrame):
                Bulk save data to fill the fields.
            fields (list):
                List of fields at the dataframe to be returned. Fields of
                type BulkSaveMicroserviceAutoFillField and
                BulkSaveLocalAutoFillField.
            microservice (PumpWoodMicroService):
                Object of the type PumpWoodMicroService.

        Returns:
            Return the dataframe with the columns filled with autofill
            data.
        """
        for field in fields:
            if isinstance(field, BulkSaveMicroserviceAutoFillField):
                data = data.pipe(
                    cls.autofill_microservice, field=field,
                    microservice=microservice)
            elif isinstance(field, BulkSaveLocalAutoFillField):
                data = data.pipe(cls.autofill_local, field=field)
        return cls.validate(data=data, fields=fields)

    @classmethod
    def get_field_cache(cls, model_class: str, pk: int, field: str) -> Any:
        """Get cache for field and value.

        Args:
            model_class (str):
                Model class of the object that will be used to fill the
                values.
            pk (int):
                Integer value associated with id.
            field (str):
                Field that should be returned value to fill the data.

        Returns:
            Return the cached field.
        """
        hash_dict = BulkSaveAutoFillFieldCacheHash(
            model_class=model_class, pk=pk, field=field)
        return PumpwoodFlaskGDiskCache.get(hash_dict)

    @classmethod
    def set_field_cache(cls, model_class: str, pk: int, field: str,
                        value: Any) -> Any:
        """Get cache for field and value.

        Args:
            model_class (str):
                Model class of the object that will be used to fill the
                values.
            pk (int):
                Integer value associated with id.
            field (str):
                Field that should be returned value to fill the data.
            value (Any):
                Value to be set on cache from fill value.

        Returns:
            Return the cached field.
        """
        hash_dict = BulkSaveAutoFillFieldCacheHash(
            model_class=model_class, pk=pk, field=field)
        return PumpwoodFlaskGDiskCache.set(
            hash_dict=hash_dict, value=value)

    @classmethod
    def autofill_local(cls, data: pd.DataFrame,
                       field: BulkSaveLocalAutoFillField) -> pd.DataFrame:
        """Add column using autofill."""
        unique_fk_columns = list(data[field.object_fk_column].unique())
        map_fk_fill_data = {}
        missing_cache = []

        # Get data from localcache if avaiable and allowed
        for fk_pk in unique_fk_columns:
            if not field.use_cache:
                missing_cache.append(fk_pk)
            else:
                cached_value = cls.get_field_cache(
                    model_class=field.fill_model_class.__name__,
                    pk=fk_pk, field=field.fill_col)
                if cached_value is None:
                    missing_cache.append(int(fk_pk))
                else:
                    map_fk_fill_data[fk_pk] = cached_value

        # Fetch data from database
        if len(missing_cache) != 0:
            fk_objects = field.fill_model_class.query_list(
                filter_dict={"id__in": missing_cache})
            for fk_obj in fk_objects:
                if not hasattr(fk_obj, field.fill_col):
                    msg = (
                        "Foreign object [{model_class}] used to fill the "
                        "value does not have the expected field [{fill_col}]."
                    ).format(
                        fill_col=field.fill_col,
                        model_class=field.fill_model_class.__name__)
                    raise PumpWoodOtherException(msg)

                fk_fill_value = getattr(fk_obj, field.fill_col)
                map_fk_fill_data[fk_obj.id] = fk_fill_value

                # Set cache
                cls.set_field_cache(
                    model_class=field.fill_model_class.__name__,
                    pk=fk_pk, field=field.fill_col, value=fk_fill_value)

        data[field.field] = \
            data[field.object_fk_column].map(map_fk_fill_data)
        return data

    @classmethod
    def autofill_microservice(cls, data: pd.DataFrame,
                              field: BulkSaveMicroserviceAutoFillField,
                              microservice: PumpWoodMicroService
                              ) -> pd.DataFrame:
        """Add column using autofill."""
        unique_fk_columns = list(data[field.object_fk_column].unique())
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

        data[field.field] = \
            data[field.object_fk_column].map(map_fk_fill_data)
        return data

    @classmethod
    def validate(cls, data: pd.DataFrame, fields: list) -> pd.DataFrame:
        """Validate data from the filled dataframe.

        Args:
            data (pd.DataFrame):
                Dataframe to be validated and have the columns filtered.
            fields (list):
                Field that should be present on the final bulk save
                dataframe.
        """
        final_cols: list[str] = []
        for x in fields:
            if isinstance(x, (BulkSaveMicroserviceAutoFillField,
                              BulkSaveLocalAutoFillField)):
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
                msg, payload={"expected_cols_bulk_save": fields})

        data_columns = set(data.columns)
        missing_cols = set(final_cols) - data_columns
        if len(missing_cols):
            msg = (
                "Some of the necessary columns are not at the buck save "
                "{missing_cols} data. Data columns {data_columns}; "
                "expected columns [{expected_columns}]")
            raise PumpWoodOtherException(
                msg, payload={
                    "missing_cols": list(missing_cols),
                    "data_columns": list(data_columns),
                    "expected_columns": list(final_cols)
                })
        return data.loc[:, final_cols]
