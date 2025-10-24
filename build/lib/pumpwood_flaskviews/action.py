"""Define actions decorator."""
import inspect
import textwrap
import pandas as pd
import typing
from datetime import date, datetime
from typing import Callable, cast
from pumpwood_communication.exceptions import PumpWoodActionArgsException


class Action:
    """Define a Action class to be used in decorator action."""

    def __init__(self, func: Callable, info: str):
        """."""
        def extract_param_type(param) -> None:
            """Extract paramter type."""
            resp = {"many": False}
            if param.annotation == inspect.Parameter.empty:
                if param.default == inspect.Parameter.empty:
                    resp["type"] = "Any"
                else:
                    resp["type"] = type(param.default).__name__
            elif isinstance(param.annotation, str):
                resp["type"] = param.annotation
            elif isinstance(param.annotation, type):
                resp["type"] = param.annotation.__name__
            elif typing.get_origin(param.annotation) == typing.Literal:
                resp["type"] = "options"
                typing_args = typing.get_args(param.annotation)
                resp["in"] = [
                    {"value": x, "description": x}
                    for x in typing_args]
            elif isinstance(typing.get_origin(param.annotation), list):
                resp["many"] = True
                list_args = typing.get_args(param.annotation)
                if len(list_args) == 0:
                    resp["type"] = "Any"
                else:
                    resp["type"] = list_args[0].__name__
            else:
                resp["type"] = str(param.annotation).replace(
                    'typing.', '')
            return resp

        def extract_return_type(return_annotation):
            """Extract result type."""
            resp = {"many": False}
            if return_annotation == inspect.Parameter.empty:
                resp["type"] = "Any"
            elif isinstance((return_annotation), str):
                resp["type"] = return_annotation
            elif isinstance(return_annotation, type):
                resp["type"] = return_annotation.__name__
            elif (typing.get_origin(return_annotation) == typing.Literal):
                resp["type"] = "options"
                typing_args = typing.get_args(return_annotation)
                resp["in"] = [
                    {"value": x, "description": x}
                    for x in typing_args]
            elif isinstance(typing.get_origin(return_annotation), list):
                resp["many"] = True
                list_args = typing.get_args(return_annotation)
                if len(list_args) == 0:
                    resp["type"] = "Any"
                else:
                    resp["type"] = list_args[0].__name__
            else:
                resp["type"] = str(return_annotation).replace(
                    'typing.', '')
            return resp

        # Getting function parameters hint
        signature = inspect.signature(func)
        function_parameters = signature.parameters
        parameters = {}
        is_static_function = True
        for key in function_parameters.keys():
            if key == "self":
                is_static_function = False
                # Does not return self parameter to user
                continue

            if key == "cls":
                # Does not return cls parameter from class functions
                continue

            param = function_parameters[key]
            param_type = extract_param_type(param)
            temp_dict = {
                "required": param.default is inspect.Parameter.empty}
            temp_dict.update(param_type)
            parameters[key] = temp_dict
            if param.default is not inspect.Parameter.empty:
                parameters[key]['default_value'] = param.default

        # Getting return types:
        return_annotation = signature.return_annotation
        self.func_return = extract_return_type(return_annotation)
        self.doc_string = textwrap.dedent(func.__doc__).strip()
        self.action_name = func.__name__
        self.is_static_function = is_static_function
        self.parameters = parameters
        self.info = info

    def to_dict(self):
        """Return dict representation of the action."""
        result = {
            "action_name": self.action_name,
            "is_static_function": self.is_static_function,
            "info": self.info,
            "return": self.func_return,
            "parameters": self.parameters,
            "doc_string": self.doc_string}
        return result


def action(info: str = ""):
    """Define decorator that will convert the function into a rest action.

    Args:
        info (str):
            Just an information about the decorated function that will be
            returned in GET /rest/<model_class>/actions/.

    Returns:
        func: Action decorator.

    """
    def action_decorator(func):
        func.is_action = True
        func.action_object = Action(func=func, info=info)
        return func
    return action_decorator


def load_action_parameters(func: Callable, parameters: dict):
    """Cast arguments to its original types."""
    signature = inspect.signature(func)
    function_parameters = signature.parameters
    return_parameters = {}
    errors = {}
    unused_params = set(parameters.keys()) - set(function_parameters.keys())
    if len(unused_params) != 0:
        errors["unused args"] = {
            "type": "unused args",
            "message": list(unused_params)}
    for key in function_parameters.keys():
        # pass if arguments are self and cls for classmethods
        if key in ['self', 'cls']:
            continue

        param_type = function_parameters[key]
        par_value = parameters.get(key)

        param_str = str(param_type.annotation).replace('typing.', '')
        if par_value is not None:
            try:
                if param_type.annotation == date:
                    return_parameters[key] = pd.to_datetime(par_value).date()
                elif param_type.annotation == datetime:
                    return_parameters[key] = \
                        pd.to_datetime(par_value).to_pydatetime()
                else:
                    if 'List' == param_str:
                        if len(param_type.annotation.__args__ != 0):
                            type_var = param_type.annotation.__args__[0]
                            return_parameters[key] = [
                                type_var(x) for x in par_value]
                        else:
                            return_parameters[key] = par_value
                    elif 'Set' == param_str:
                        if len(param_type.annotation.__args__ != 0):
                            type_var = param_type.annotation.__args__[0]
                            return_parameters[key] = set([
                                type_var(x) for x in par_value])
                        else:
                            return_parameters[key] = set(par_value)
                    else:
                        return_parameters[
                            key] = param_type.annotation(par_value)

            # If any error ocorrur then still try to cast data from python
            # typing
            except Exception:
                try:
                    return_parameters[key] = return_parameters[key] = cast(
                        param_type.annotation, par_value)
                except Exception as e:
                    errors[key] = {
                        "type": "unserialize",
                        "message": str(e)}
        # If parameter is not passed and required return error
        elif param_type.default is inspect.Parameter.empty:
            errors[key] = {
                "type": "nodefault",
                "message": "not set and no default"}

    if len(errors.keys()) != 0:
        template = "[{key}]: {message}"
        error_msg = "error when unserializing function arguments:\n"
        error_list = []
        for key in errors.keys():
            error_list.append(template.format(
                key=key, message=errors[key]["message"]))
        error_msg = error_msg + "\n".join(error_list)
        raise PumpWoodActionArgsException(
            status_code=400, message=error_msg, payload={
                "arg_errors": errors})
    return return_parameters
