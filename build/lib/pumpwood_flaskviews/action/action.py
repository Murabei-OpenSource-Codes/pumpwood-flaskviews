"""Define actions decorator and class."""
import inspect
import textwrap
from typing import Callable
from pumpwood_communication.type import ActionInfomation
from pumpwood_flaskviews.action.inspect import InspectType


class Action:
    """Internal class used to metadata-decorate a PumpWood action.

    Extracts signature, parameters, and return information from a
    wrapped function to expose it via the API.
    """

    def __init__(self, func: Callable, info: str, required_role: str):
        """Initialize the Action metadata by inspecting the function.

        Args:
            func (Callable):
                The function to be exposed as an action.
            info (str):
                A human-readable description of the action.
            required_role (str):
                The role required to execute the action. If 'default',
                it inherits endpoint permissions from PumpWood Auth.
        """
        # Unwrapp the function in case of the function be also anootated
        # by another decorator
        unwrapped_func = inspect.unwrap(func)
        signature = inspect.signature(unwrapped_func)

        # Getting function parameters hints to be returned on
        # list actions inforamtion
        function_parameters = signature.parameters
        parameters = {}
        is_static_function = True
        for key in function_parameters.keys():
            # Does not return self parameter to user
            if key == "self":
                is_static_function = False
                continue

            # Does not return cls parameter from class functions
            if key == "cls":
                continue

            param = function_parameters[key]
            param_type = InspectType\
                .extract_parameter_type(param)
            parameters[key] = param_type

        # Get return type to return on list actions
        return_annotation = signature.return_annotation
        mock_return_param = inspect.Parameter(
            name="return", kind=inspect.Parameter.POSITIONAL_ONLY,
            annotation=return_annotation)
        self.func_return = InspectType\
            .extract_return_type(mock_return_param)

        # Add other information associated with the action
        raw_doc = func.__doc__ or "**Empty doc string**"
        self.doc_string = textwrap.dedent(raw_doc).strip()
        self.action_name = func.__name__
        self.is_static_function = is_static_function
        self.parameters = parameters
        self.info = info
        self.required_role = required_role

    def to_dict(self) -> dict:
        """Return a dictionary representation of the action metadata."""
        result = ActionInfomation(
            action_name=self.action_name,
            is_static_function=self.is_static_function,
            info=self.info,
            return_=self.func_return,
            parameters=self.parameters,
            doc_string=self.doc_string,
            required_role=self.required_role).to_dict()
        return result


def action(info: str = "", required_role: str = 'default'):
    """Decorator to expose a function as a PumpWood API action.

    Args:
        info (str):
            Description returned in the actions list.
        required_role (str):
            The role required to run the action.

    Returns:
        Callable:
            The wrapped function with action metadata attached.
    """
    def action_decorator(func):
        func.is_action = True
        func.action_object = Action(
            func=func, info=info, required_role=required_role)
        return func
    return action_decorator
