from contextlib import asynccontextmanager
from typing import ContextManager, Callable, TypeVar, Union
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker as sqlalchemy_sessionmaker, Session as NormalSession


T = TypeVar('T', bound=Callable[[], Union[AsyncSession, NormalSession]])


def sessionmaker(bind, class_: T) -> T:
    return sqlalchemy_sessionmaker(bind, class_)


conn_str = 'sqlite+aiosqlite:///db.sqlite'
engine = create_async_engine(conn_str)


def Session(*args, **kwargs):
    return AsyncSession(bind=engine, *args, **kwargs)


@asynccontextmanager
async def session_scope() -> ContextManager[AsyncSession]:
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        await session.commit()
    except:
        await session.rollback()
        raise
    finally:
        await session.close()