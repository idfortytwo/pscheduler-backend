import datetime
import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from api.app import app
from db.connection import sessionmaker
from db.dal import DAL, get_dal
from db.models import Base
from scheduler.task import IntervalTask, CronTask, DateTask


engine = create_async_engine('sqlite+aiosqlite:///test_db.sqlite')
TestSession = sessionmaker(bind=engine, class_=AsyncSession)


async def get_dal_override():
    async with TestSession(expire_on_commit=False) as session:
        async with session.begin():
            yield DAL(session)


app.dependency_overrides[get_dal] = get_dal_override
client = TestClient(app)


@pytest.fixture
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def session(setup_db):
    db_session = TestSession()
    yield db_session
    await db_session.rollback()


@pytest.fixture
async def add_one_task(session):
    session.add(IntervalTask('echo 1s', seconds=1))
    await session.commit()


@pytest.fixture
async def add_three_tasks(session):
    session.add(IntervalTask('echo 1s', seconds=1))
    session.add(CronTask('echo cron', '1 0 * * *'))
    session.add(DateTask('echo date', date=datetime.datetime.utcnow()))
    await session.commit()


def to_json(data):
    return json.dumps(data, separators=(',', ':'))


__all__ = ['client', 'to_json', 'setup_db', 'session', 'add_one_task', 'add_three_tasks']