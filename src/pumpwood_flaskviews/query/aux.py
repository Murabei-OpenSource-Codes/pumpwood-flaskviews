"""Aux functions for queries."""
from flask import request


def get_base_filter_skip() -> str:
    """Return the raw ``base_filter_skip`` query parameter.

    Returns:
        str:
            Parameter value from the current request, or ``'[]'`` when
            absent. Callers must parse JSON (see ``BaseQueryABC``).
    """
    return request.args.get('base_filter_skip', '[]')