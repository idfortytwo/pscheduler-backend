import sqlalchemy.orm

from contextlib import contextmanager
from typing import ContextManager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


engine = create_engine('sqlite:///db.sqlite')
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope() -> ContextManager[sqlalchemy.orm.Session]:
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()