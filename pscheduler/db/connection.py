import os

from contextlib import asynccontextmanager
from typing import ContextManager, Callable, TypeVar, Union
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker as sqlalchemy_sessionmaker, Session as NormalSession


T = TypeVar('T', bound=Callable[[], Union[AsyncSession, NormalSession]])


def sessionmaker(bind, class_: T) -> T:
    return sqlalchemy_sessionmaker(bind, class_)


try:
    load_dotenv()
    user = os.environ['DB_USER']
    pwd = os.environ['DB_PASS']
    host = os.environ['DB_HOST']
    port = os.environ['DB_PORT']
    database = os.environ['DB_DATABASE']
    conn_str = f"postgresql+asyncpg://{user}:{pwd}@{host}:{port}/{database}"

    engine = create_async_engine(conn_str)
except KeyError as e:
    print(f'Missing key {e.args[0]} in connection config')
    exit(1)


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