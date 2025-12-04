"""Module to help connection with SQLAlchemy and create sessions."""
from sqlalchemy.sql import text


def get_session(db):
    """Ping connection before using database.

    Ping connection before quering database and restore session if
    necessary.

    Args:
        db:
            Database used on Flask SQLAlchemy connection.

    Returns:
        A SQLAlchemy session.
    """
    session = db.session
    try:
        session.execute(text("SELECT 1;"))
    except Exception:
        db.engine.dispose()
        session.rollback()
    return session
