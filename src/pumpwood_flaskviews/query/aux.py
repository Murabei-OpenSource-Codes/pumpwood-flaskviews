"""Aux functions for queries."""
from flask import request


def get_base_filter_skip() -> list[str]:
    """Get skip argument from request."""
    return request.args.get('base_filter_skip', '[]')