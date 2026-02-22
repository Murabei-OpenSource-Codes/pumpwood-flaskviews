"""Auxiliary functions for fill options and fields information."""
import copy
import inspect
from typing import Any, Literal
from marshmallow import missing
from sqlalchemy import (
    inspect as sqlalchemy_inspect, Integer)
from geoalchemy2.types import Geometry
from sqlalchemy.sql.functions import GenericFunction, Function
from sqlalchemy.sql.schema import Sequence
from sqlalchemy.sql.expression import False_ as sql_false
from sqlalchemy.sql.expression import True_ as sql_true
from sqlalchemy_utils.types.choice import ChoiceType
from pumpwood_i8n.singletons import pumpwood_i8n as _
from pumpwood_flaskviews.config import INFO_CACHE_TIMEOUT
from pumpwood_communication.cache import default_cache
from pumpwood_communication.type import (
    MISSING, AUTOINCREMENT, NOW, TODAY, ColumnInfo,
    ColumnExtraInfo, FileColumnExtraInfo, OptionsColumnExtraInfo,
    PumpwoodMissingType, PrimaryKeyExtraInfo)


class AuxFillOptions:
    """Help to extract information from fields on model class."""

    HASH_DICT = {
        "context": "pumpwood_flaskviews",
        "end-point": "cls_fields_options",
        "model_class": None,
        "user_type": None}
    """Base of the hash dict."""

    TRANSLATION_TAG_TEMPLATE = "{model_class}__fields__{field}"
    """Translation tag for verbose fields."""

    @classmethod
    def run(cls, model_class: object, serializer,
            view_file_fields: dict = None,
            user_type: Literal['api', 'gui'] = 'api') -> dict[str, dict]:
        """Extract the information.

        Args:
            model_class:
                Model class that information from the fields will be
                retrieved.
            serializer:
                The class of the serializer used for this models class.
            view_file_fields (dict):
                View associated file fields.
            user_type (Literal['api', 'gui']):
                User type that resquest the field type.

        Returns:
            Return a dictonary with keys of the fields and values a dictonary
            with description o the column.
        """
        model_class_name = cls.get_model_class_name(
            model_class=model_class)
        if view_file_fields is None:
            view_file_fields = {}

        # Retrieve local cache if avaiable
        hash_dict = cls.get_hash_dict(
            model_class_name=model_class_name,
            user_type=user_type)
        cached_data = cls.fetch_cache(hash_dict=hash_dict)
        if cached_data is not None:
            return cached_data

        # Generate data if cache is invalid
        mapper = cls.get_model_class_mapper(
            model_class=model_class)
        serializer_fields_data = cls.get_serializer_fields(
            serializer=serializer)
        serializer_fields = serializer_fields_data['serializer_fields']
        foreign_keys = serializer_fields_data['foreign_keys']
        related_fields = serializer_fields_data['related_fields']
        gui_readonly = serializer_fields_data['gui_readonly']

        # Retrieve information from table fields
        column_data = {}
        for column in mapper.columns:
            column_name = column.name
            temp_field = serializer_fields.get(column_name)
            info_dict = cls.create_field_info_dict(
                column_name=column_name,
                model_class_name=model_class_name,
                column=column,
                field_data=temp_field,
                view_file_fields=view_file_fields,
                foreign_keys=foreign_keys,
                gui_readonly=gui_readonly,
                user_type=user_type)
            column_data[column_name] = info_dict

        # Related models that will be returned with serialization
        for column_name, related_info in related_fields.items():
            temp_field = serializer_fields.get(column_name)
            info_dict = cls.create_related_field_info_dict(
                model_class_name=model_class_name,
                column_name=column_name,
                field_data=temp_field,
                related_info=related_info)
            column_data[column_name] = info_dict

        # Get table partitions from model class and create the definition
        # of the primary key data
        table_partitions = cls.get_table_partitions(
            model_class=model_class)
        info_dict = cls.create_pk_info_dict(
            model_class_name=model_class_name,
            column_data=column_data,
            table_partitions=table_partitions)
        column_data['pk'] = info_dict

        # Set diskcache to reduce calls
        cls.set_cache(hash_dict=hash_dict, data=column_data)
        return column_data

    @classmethod
    def get_hash_dict(cls, model_class_name: str, user_type: str) -> dict:
        """Get a base hash dict."""
        hash_dict = copy.deepcopy(cls.HASH_DICT)
        hash_dict['model_class'] = model_class_name
        hash_dict['user_type'] = user_type
        return hash_dict

    @classmethod
    def fetch_cache(cls, hash_dict: str) -> dict[str, ColumnInfo] | None:
        """Fetch information about the fields from the local cache."""
        return default_cache.get(hash_dict=hash_dict)

    @classmethod
    def set_cache(cls, hash_dict: str, data: dict[str, ColumnInfo]) -> bool:
        """Set information about the fields at the local cache."""
        return default_cache.set(
            hash_dict=hash_dict, value=data,
            expire=INFO_CACHE_TIMEOUT)

    @classmethod
    def get_model_class_name(cls, model_class) -> str:
        """Get lower case name for the model_class class."""
        if inspect.isclass(model_class):
            return model_class.__name__.lower()
        return model_class.__class__.__name__.lower()

    @classmethod
    def get_verbose_tag(cls, model_class_name: str, column_name: str) -> str:
        """Get model class columns."""
        return cls.TRANSLATION_TAG_TEMPLATE.format(
            model_class=model_class_name,
            field=column_name)

    @classmethod
    def get_model_class_mapper(cls, model_class):
        """Get model class columns."""
        return sqlalchemy_inspect(model_class)

    @classmethod
    def get_table_partitions(cls, model_class):
        """Get table partitions from mapper."""
        return getattr(model_class, 'table_partition', [])

    @classmethod
    def get_serializer_fields(cls, serializer):
        """Get serializer fields."""
        # Create serializer with FK and related to retrieve information
        serializer_obj = serializer(
            foreign_key_fields=True, related_fields=True)
        serializer_fields = serializer_obj.fields
        foreign_keys = serializer_obj.get_foreign_keys()
        related_fields = serializer_obj.get_related_fields()
        gui_readonly = serializer_obj.get_gui_readonly()
        return {
            'serializer_fields': serializer_fields,
            'foreign_keys': foreign_keys,
            'related_fields': related_fields,
            'gui_readonly': gui_readonly
        }

    @classmethod
    def get_nullable(cls, column, field_data) -> bool:
        """Get if column can be considered nullable."""
        if field_data is not None:
            return getattr(field_data, 'allow_none')
        else:
            return column.nullable

    @classmethod
    def get_default(cls, column, field_data) -> Any | PumpwoodMissingType:
        """Get default value for the column."""
        col_type = type(column.type)

        # Check if the column is an autoincrementing primary key
        is_id_autoincrement = (
            column.autoincrement in (True, 'auto') and
            issubclass(col_type, Integer))
        if is_id_autoincrement:
            return AUTOINCREMENT

        # Check if there is a default information at serializer
        ser_field_default = MISSING
        if field_data is not None:
            # Custom attribute to help with calculated custom fields on
            # pumpwood
            pumpwood_read_only = getattr(
                field_data, 'pumpwood_read_only', False)
            ser_field_default = getattr(field_data, 'load_default')

            # If dump default is not vaiable
            if ser_field_default is missing:
                if pumpwood_read_only:
                    ser_field_default = getattr(
                        field_data, 'pumpwood_default', MISSING)
                else:
                    ser_field_default = MISSING

        # If a default is set on serializer level, use it
        if ser_field_default is not MISSING:
            return ser_field_default

        # Try to get the default from the default at SQLAlchemy level
        column_default = column.default
        if column_default is not None:
            arg = getattr(column_default, 'arg', None)

            if isinstance(arg, (GenericFunction, Function)):
                name = getattr(arg, 'name', '').lower()
                if name in ('now', 'current_timestamp'):
                    return NOW
                elif name in ('current_date', 'today'):
                    return TODAY
                return getattr(arg, 'name', str(arg))

            elif inspect.isroutine(arg):
                name = getattr(arg, '__name__', '').lower()
                if name in ('now', 'utcnow', 'current_timestamp'):
                    return NOW
                elif name in ('today', 'current_date'):
                    return TODAY
                return arg.__name__ + '()'

            elif arg is dict:
                return {}
            elif isinstance(column.default, Sequence):
                return AUTOINCREMENT
            elif isinstance(arg, sql_false):
                return False
            elif isinstance(arg, sql_true):
                return True
            else:
                return arg

        # Try to get the server_default from the default at Database level
        server_default = column.server_default
        if server_default is not None:
            arg = getattr(server_default, 'arg', None)

            if isinstance(arg, (GenericFunction, Function)):
                name = getattr(arg, 'name', '').lower()
                if name in ('now', 'current_timestamp'):
                    return NOW
                elif name in ('current_date', 'today'):
                    return TODAY
                return getattr(
                    arg, 'description',
                    getattr(arg, 'name', str(arg)))

            if hasattr(arg, 'text'):
                text_lower = str(arg.text).lower()
                if 'now' in text_lower or 'current_timestamp' in text_lower:
                    return NOW
                elif 'current_date' in text_lower or 'today' in text_lower:
                    return TODAY
                return str(arg.text)

            elif inspect.isroutine(arg):
                name = getattr(arg, '__name__', '').lower()
                if name in ('now', 'utcnow', 'current_timestamp'):
                    return NOW
                elif name in ('today', 'current_date'):
                    return TODAY
                return arg.__name__ + '()'

            elif arg is dict:
                return {}
            elif isinstance(column.server_default, Sequence):
                return AUTOINCREMENT
            elif isinstance(arg, sql_false):
                return False
            elif isinstance(arg, sql_true):
                return True
            else:
                return arg
        return MISSING

    @classmethod
    def get_read_only(cls, column, field_data, gui_readonly,
                      user_type: str = 'api') -> bool:
        """Get read_only value for the column."""
        dump_only = getattr(
            field_data, 'dump_only', False)
        # Custom attribute to help with calculated custom fields on
        # pumpwood
        pumpwood_read_only = getattr(
            field_data, 'pumpwood_read_only', False)
        read_only = dump_only or pumpwood_read_only

        if user_type == 'gui':
            read_only = (
                read_only or
                (column.name in gui_readonly))
        return read_only

    @classmethod
    def get_type(cls, column, view_file_fields: dict,
                 foreign_keys: dict) -> str:
        """Get column type."""
        # Check for auxiliary data for more information
        temp_view_file_fields = view_file_fields or {}
        file_types = temp_view_file_fields.get(column.name)
        fk_data = foreign_keys.get(column.name)

        if isinstance(column.type, Geometry):
            return "geometry"
        if isinstance(column.type, ChoiceType):
            return "options"
        if file_types is not None:
            return "file"
        if fk_data is not None:
            return "foreign_key"
        else:
            return column.type.python_type.__name__

    @classmethod
    def get_help_text(cls, column) -> bool:
        """Get help text."""
        if column.name == 'id':
            return AUTOINCREMENT.help_text()
        else:
            return column.doc

    @classmethod
    def get_column_verbose(cls, column_name: str, verbose_tag: str) -> str:
        """Get column name verbose."""
        return _.t(
            sentence=column_name,
            tag=verbose_tag + "__column")

    @classmethod
    def get_help_text_verbose(cls, verbose_tag: str, help_text: str) -> str:
        """Get help text name verbose."""
        return _.t(
            sentence=help_text,
            tag=verbose_tag + "__help_text")

    @classmethod
    def get_column_name(cls, column) -> str:
        """Get column name verbose."""
        return column.name

    @classmethod
    def get_primary_key(cls, column) -> bool:
        """Get primary key."""
        return column.primary_key

    @classmethod
    def get_unique(cls, column) -> bool:
        """Get unique."""
        is_unique = getattr(column, 'unique', False)
        if is_unique is None:
            is_unique = False
        return is_unique

    @classmethod
    def _build_options_data(cls, verbose_tag: str, column) -> dict:
        """Return the options associated with the field."""
        if isinstance(column.type, ChoiceType):
            in_dict = {}
            for choice in column.type.choices:
                in_dict[choice[0]] = {
                    "value": choice[0],
                    "description__verbose": _.t(
                        sentence=choice[1],
                        tag=verbose_tag + "__choice__" + str(choice[0])
                    ),
                    "description": choice[1]}
            return in_dict
        else:
            return MISSING

    @classmethod
    def get_in(cls, column, verbose_tag) -> bool:
        """Get unique."""
        return cls._build_options_data(column=column, verbose_tag=verbose_tag)

    @classmethod
    def get_extra_info(cls, type_str: str, column, field_data,
                       view_file_fields, foreign_keys,
                       verbose_tag: str) -> ColumnExtraInfo:
        """Get extra info for fields."""
        if type_str == 'foreign_key':
            foreign_key_field_data = foreign_keys.get(column.name)
            if foreign_key_field_data is not None:
                return foreign_key_field_data
            else:
                raise Exception("Something is not implemented correctly")

        if type_str == 'options':
            in_data = cls._build_options_data(
                column=column, verbose_tag=verbose_tag)
            return OptionsColumnExtraInfo(
                in_=in_data)

        if type_str == 'file':
            permited_file_types = view_file_fields.get(column.name)
            if permited_file_types is not None:
                return FileColumnExtraInfo(
                    permited_file_types=permited_file_types)
            else:
                raise Exception("Something is not implemented correctly")
        return {}

    @classmethod
    def get_indexed(cls, column) -> bool:
        """Return if column is indexed."""
        is_indexed = getattr(column, 'index', False)
        if is_indexed is None:
            is_indexed = False
        return (
            is_indexed
            or column.primary_key
            or column.unique)

    @classmethod
    def get_primary_keys(cls, column_data) -> dict:
        """Get primary keys columns."""
        # Filter the columns that as marked as primary key
        return [
            key for key, item in column_data.items()
            if item['primary_key']]

    @classmethod
    def create_field_info_dict(cls, column_name: str, model_class_name: str,
                               column, field_data, view_file_fields,
                               foreign_keys, gui_readonly,
                               user_type: str = 'api') -> dict:
        """Create field info dictonary."""
        # Information to return
        verbose_tag = cls.get_verbose_tag(
            column_name=column_name,
            model_class_name=model_class_name)

        nullable = cls.get_nullable(
            column=column, field_data=field_data)
        default = cls.get_default(
            column=column,
            field_data=field_data)
        read_only = cls.get_read_only(
            column=column, field_data=field_data,
            gui_readonly=gui_readonly, user_type=user_type)
        type_str = cls.get_type(
            column=column, view_file_fields=view_file_fields,
            foreign_keys=foreign_keys)
        help_text = cls.get_help_text(
            column=column)
        primary_key = cls.get_primary_key(
            column=column)
        unique = cls.get_unique(
            column=column)
        column_in = cls.get_in(
            column=column, verbose_tag=verbose_tag)
        indexed = cls.get_indexed(
            column=column)

        # Verbose data
        column__verbose = cls.get_column_verbose(
            column_name=column_name, verbose_tag=verbose_tag)
        help_text__verbose = cls.get_help_text_verbose(
            help_text=help_text, verbose_tag=verbose_tag)

        extra_info = cls.get_extra_info(
            type_str=type_str, column=column,
            field_data=field_data, view_file_fields=view_file_fields,
            foreign_keys=foreign_keys, verbose_tag=verbose_tag)
        column_info = ColumnInfo(
            primary_key=primary_key, column=column_name,
            column__verbose=column__verbose, help_text=help_text,
            help_text__verbose=help_text__verbose, type_=type_str,
            nullable=nullable, read_only=read_only, unique=unique,
            extra_info=extra_info, in_=column_in, default=default,
            indexed=indexed)
        return column_info.to_dict()

    @classmethod
    def create_related_field_info_dict(cls, model_class_name: str,
                                       column_name: str, field_data,
                                       related_info) -> dict:
        """Create related field info dictonary."""
        verbose_tag = cls.get_verbose_tag(
            column_name=column_name,
            model_class_name=model_class_name)

        nullable = True
        default = MISSING
        read_only = field_data.read_only
        type_str = 'related_model'
        help_text = field_data.help_text
        column__verbose = cls.get_column_verbose(
            column_name=column_name, verbose_tag=verbose_tag)
        help_text__verbose = cls.get_help_text_verbose(
            help_text=help_text, verbose_tag=verbose_tag)
        primary_key = False
        unique = False
        column_in = MISSING
        extra_info = related_info
        column_info = ColumnInfo(
            primary_key=primary_key, column=column_name,
            column__verbose=column__verbose, help_text=help_text,
            help_text__verbose=help_text__verbose, type_=type_str,
            nullable=nullable, read_only=read_only, unique=unique,
            extra_info=extra_info, in_=column_in, default=default,
            indexed=False)
        return column_info.to_dict()

    @classmethod
    def create_pk_info_dict(cls, model_class_name: str,
                            table_partitions: list[str],
                            column_data: dict) -> dict:
        """Create primary key column information."""
        verbose_tag = cls.get_verbose_tag(
            column_name='pk',
            model_class_name=model_class_name)

        nullable = False
        default = MISSING
        read_only = False
        column_name = 'pk'
        help_text = (
            'Primary key used to retrieve and filter data. It is `virtual` '
            'column check its definition at extra_info `columns`.')
        column__verbose = cls.get_column_verbose(
            column_name=column_name, verbose_tag=verbose_tag)
        help_text__verbose = cls.get_help_text_verbose(
            help_text=help_text, verbose_tag=verbose_tag)
        primary_key = False
        unique = True
        column_in = MISSING
        primary_keys = cls.get_primary_keys(
            column_data=column_data)

        type_str = 'int'
        if 1 < len(primary_keys):
            type_str = 'base64'

        # Create columns information to be served at options
        extra_info = PrimaryKeyExtraInfo(
            columns=primary_keys,
            partition=table_partitions)
        column_info = ColumnInfo(
            primary_key=primary_key, column=column_name,
            column__verbose=column__verbose, help_text=help_text,
            help_text__verbose=help_text__verbose, type_=type_str,
            nullable=nullable, read_only=read_only, unique=unique,
            extra_info=extra_info, in_=column_in, default=default,
            indexed=True)
        return column_info.to_dict()
