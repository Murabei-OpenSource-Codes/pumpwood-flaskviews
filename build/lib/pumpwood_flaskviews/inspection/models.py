"""Module to inspect models"""
from sqlalchemy import inspect as alchemy_inspect


def model_has_column(model, column: str):
    """Check if model has column."""
    mapper = alchemy_inspect(model)
    return column in mapper.columns
