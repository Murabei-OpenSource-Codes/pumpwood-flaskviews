"""Module for custom sqlalchemy fields."""
from sqlalchemy_utils.types.choice import ChoiceType


class CacheableChoiceType(ChoiceType):
    """Create a Choice Type that will cache options from choice type."""

    cache_ok = True  # Mark this type as cacheable


def get_session(self):
    """Ping connection before using database.

    Ping connection before quering database and restore session if
    necessary.
    """
    session = self.db.session
    try:
        session.execute(text("SELECT 1;"))
    except Exception:
        self.db.engine.dispose()
        session.rollback()
    return session
