"""Module for custom sqlalchemy fields."""
from sqlalchemy_utils.types.choice import ChoiceType


class CacheableChoiceType(ChoiceType):
    """Create a Choice Type that will cache options from choice type."""

    cache_ok = True  # Mark this type as cacheable
