from contextlib import asynccontextmanager
from typing import ContextManager, Callable, TypeVar
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker as sqlalchemy_sessionmaker


T = TypeVar('T', bound=Callable[[], AsyncSession])


def sessionmaker(bind, class_: T) -> T:
    return sqlalchemy_sessionmaker(bind, class_)


engine = create_async_engine('sqlite+aiosqlite:///db.sqlite')
Session = sessionmaker(bind=engine, class_=AsyncSession)


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