import typing
import inspect
from pumpwood_communication.type import ActionParameterType, ActionReturnType


class InspectType:
    """Inspect the argument and return type and normalize the result."""

    @classmethod
    def extract_type(cls, param: inspect.Parameter) -> dict:
        """Extract parameter type information."""
        parameter_many = False
        parameter_type = "Any"
        parameter_in = None

        # Determine if required: It's required if there's no default value
        parameter_required = param.default is inspect.Parameter.empty
        annotation = param.annotation

        if annotation is inspect.Parameter.empty:
            parameter_type = "Any"

        elif isinstance(annotation, str):
            parameter_type = annotation

        elif isinstance(annotation, type):
            parameter_type = annotation.__name__

        else:
            origin = typing.get_origin(annotation)
            args = typing.get_args(annotation)

            # Handle Literal (Options)
            if origin is typing.Literal:
                parameter_type = "options"
                parameter_in = [
                    {"value": x, "description": "empty"
                     if x is None else str(x)}
                    for x in args
                ]

            # Handle List/Collections
            elif origin in (list, typing.List, set, typing.Set):
                parameter_many = True
                if args:
                    # Handle nested types (e.g., list[int])
                    inner_type = args[0]
                    parameter_type = getattr(
                        inner_type, "__name__", str(inner_type))
                else:
                    parameter_type = "Any"

            # Handle Union/Optional (e.g., Optional[int] -> int)
            elif origin is typing.Union:
                # Filter out NoneType to get the real type
                non_none_args = [
                    a for a in args if a is not type(None)]
                if non_none_args:
                    first_arg = non_none_args[0]
                    parameter_type = getattr(
                        first_arg, "__name__", str(first_arg))

            else:
                # Fallback for complex typing objects
                parameter_type = str(annotation).replace('typing.', '')

        return {
            "many": parameter_many,
            "type": parameter_type,
            "in": parameter_in,
            "required": parameter_required
        }

    @classmethod
    def get_parameter_details(cls, param: inspect.Parameter) -> dict:
        """Helper to get default value and requirement status."""
        is_empty = param.default is inspect.Parameter.empty
        return {
            "default_value": None if is_empty else param.default,
            "is_required": is_empty
        }

    @classmethod
    def extract_parameter_type(cls, param: inspect.Parameter) -> dict:
        """Extract parameter type for ActionParameterType."""
        type_info = cls.extract_type(param=param)
        details = cls.get_parameter_details(param=param)

        return ActionParameterType(
            many=type_info['many'],
            type_=type_info['type'],
            in_=type_info['in'],
            required=details['is_required'],
            default_value=details['default_value']
        ).to_dict()

    @classmethod
    def extract_return_type(cls, param: inspect.Parameter) -> dict:
        """Extract return type for ActionReturnType."""
        type_info = cls.extract_type(param=param)
        return ActionReturnType(
            many=type_info['many'],
            type_=type_info['type'],
            in_=type_info['in']
        ).to_dict()
