import datetime
import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import create_async_engine

from api.app import app

from db import connection
from db.connection import Session
from db.models import Base
from scheduler.task import IntervalTask, CronTask, DateTask


test_conn_str = connection.conn_str = 'sqlite+aiosqlite:///test_db.sqlite'
test_engine = connection.engine = create_async_engine(test_conn_str)


client = TestClient(app)


@pytest.fixture
async def setup_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def session(setup_db):
    db_session = Session()
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