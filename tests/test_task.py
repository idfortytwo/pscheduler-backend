import datetime
import pytest

from typing import List
from sqlalchemy import select

from scheduler.task import IntervalTask, Task, CronTask, DateTask
from tests.testing import *


pytestmark = pytest.mark.asyncio


class TestTask:
    async def test_select_polymorphism(self, session):
        session.add(IntervalTask('interval', 'echo 1s', seconds=1))
        session.add(CronTask('cron', 'echo cron', '1 0 * * *'))
        session.add(DateTask('date', 'echo date', date=datetime.datetime.utcnow()))

        tasks: List[Task] = (await session.execute(
            select(Task).
            order_by(Task.task_id)
        )).scalars().all()
        assert type(tasks[0]) == IntervalTask and type(tasks[1]) == CronTask and type(tasks[2]) == DateTask

    async def test_get_one(self, session, add_three_tasks):
        response = client.get('/task/2')

        task: Task = (await session.execute(
            select(Task).
            filter(Task.task_id == 2)
        )).scalar()
        assert response.content.decode() == to_json({'task': task.to_dict()})

    async def test_get_all(self, session, add_three_tasks):
        response = client.get('/task')

        tasks: List[Task] = (await session.scalars(select(Task))).all()
        assert response.content.decode() == to_json({
            'tasks': [task.to_dict() for task in tasks]
        })

    async def test_insert(self, session):
        client.post(
            '/task',
            json={
                'title': 'every 1s',
                'descr': 'some descr',
                'command': 'echo 1s',
                'trigger_type': 'interval',
                'trigger_args': {
                    'seconds': 1
                }
            }
        )
        tasks: List[Task] = (await session.scalars(select(Task))).all()
        assert tasks == [IntervalTask('every 1s', 'echo 1s', seconds=1, descr='some descr')]

    async def test_delete(self, session, add_one_task):
        client.delete('/task/1')

        tasks: List[Task] = (await session.scalars(select(Task))).all()
        assert tasks == []

    async def test_update(self, session, add_one_task):
        client.post(
            '/task/1',
            json={
                'title': 'every 65s',
                'descr': 'some descr',
                'command': 'echo 65s',
                'trigger_type': 'interval',
                'trigger_args': {
                    'seconds': 5,
                    'minutes': 1
                }
            }
        )
        tasks: List[Task] = (await session.scalars(select(Task))).all()
        assert tasks == [IntervalTask('every 65s', 'echo 65s', seconds=5, minutes=1, descr='some descr')]