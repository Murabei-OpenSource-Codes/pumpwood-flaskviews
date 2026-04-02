"""Load parameter to be used on function call."""
import inspect
import pandas as pd
import typing
from datetime import date, datetime
from pumpwood_communication.exceptions import PumpWoodActionArgsException


class LoadActionParameters:
    """Load action parameter to correct python types acording to tips."""

    @classmethod
    def load(cls, func: typing.Callable, parameters: dict):
        """Cast JSON arguments to their annotated Python types.

        Args:
            func:
                Function that will have it's aruments loaded in the correct
                types.
            parameters:
                Parameters of the function to be loaded.
        """
        unwrapped_func = inspect.unwrap(func)
        signature = inspect.signature(unwrapped_func)
        function_parameters = signature.parameters

        # Loop over all parameters and try to convert them to
        # the correct python objects
        errors = {}
        return_parameters = {}
        for key, value in parameters.items():
            if key not in function_parameters:
                errors[key] = {
                    "type": "unused",
                    "message": "Unexpected argument."}
                continue

            if key in ['self', 'cls']:
                msg = (
                    "'self' and 'cls' should not be passed as arguments " +
                    "to actions.")
                errors[key] = {
                    "type": "unused",
                    "message": msg}
                continue

            param_info = function_parameters[key]
            try:
                if value is None:
                    return_parameters[key] = None
                else:
                    return_parameters[key] = \
                        cls._cast_value(param_info.annotation, value)
            except Exception as e:
                errors[key] = {
                    "type": "unserialize",
                    "message": str(e)}

        # Validate if all necessary parameters were passed as arguments
        for key, param in function_parameters.items():
            # self and cls are not valid arguments
            if key in ['self', 'cls']:
                continue

            is_parameter_missing = (
                (key not in return_parameters) and
                (param.default is inspect.Parameter.empty))
            if is_parameter_missing:
                errors[key] = {
                    "type": "missing",
                    "message": "Required parameter missing."}

        if errors:
            cls._raise_arg_exception(errors)
        return return_parameters

    @classmethod
    def _cast_value(cls, annotation, val):
        """The recursive engine that maps JSON types to Python types."""
        # If no type hint is provided, return as is
        if annotation in (inspect.Parameter.empty, typing.Any):
            return val

        origin = typing.get_origin(annotation)
        args = typing.get_args(annotation)

        # Handle Date/Datetime (JSON sends these as strings)
        if annotation is date:
            return pd.to_datetime(val).date()
        if annotation is datetime:
            return pd.to_datetime(val).to_pydatetime()

        # Handle Collections (List, Set, Tuple)
        if origin in (list, typing.List, set, typing.Set, tuple, typing.Tuple):
            cast_origin = origin if origin is not typing.List else list
            if args:
                # Recursively cast every item in the list/set
                inner_type = args[0]
                return cast_origin(
                    cls._cast_value(inner_type, item)
                    for item in val)
            return cast_origin(val)

        if origin is typing.Union:
            non_none_types = [a for a in args if a is not type(None)]

            # If it is exactly Optional[X], we know exactly what to cast it to
            if len(non_none_types) == 1:
                return cls._cast_value(non_none_types[0], val)
            else:
                msg = (
                    "Union of more than one type that is not None, is "
                    "unpredictable and not castable. At this case use Any and "
                    "and assume the treatment inside the function.")
                raise Exception(msg)
            return val

        # Handle Primitives & Booleans
        if isinstance(annotation, type):
            if annotation is bool and isinstance(val, str):
                # JSON/URL params often send booleans as "true"/"false" strings
                return val.lower() not in ('false', '0', 'no', 'f', 'n')
            return annotation(val)
        return val

    @classmethod
    def _raise_arg_exception(cls, errors):
        """Helper to format the final exception message."""
        fields_with_error = []
        for k, v in errors.items():
            fields_with_error.append(k)

        msg = (
            "Error unserializing function arguments for fields "
            "{fields_with_error}")
        raise PumpWoodActionArgsException(
            message=msg, payload={
                "arg_errors": errors,
                "fields_with_error": fields_with_error})
