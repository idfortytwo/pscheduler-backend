import asyncio

import pytest
from sqlalchemy import select
from db.models import ExecutionLog, ExecutionOutputLog
from scheduler.executor import ExecutionManager
from scheduler.task import IntervalTask
from tests.testing import event_loop, client, session, add_one_task, setup_db  # noqa
from util import TaskOutputLogger


logger = TaskOutputLogger()
pytestmark = pytest.mark.asyncio


@pytest.fixture
async def execution_manager():
    execution_manager = ExecutionManager()
    await execution_manager.sync()
    yield execution_manager
    execution_manager.stop_all()
    execution_manager.task_executors.clear()


class TestExecutorManager:
    async def test_empty(self, session, execution_manager):
        assert len(execution_manager.task_executors) == 0

    async def test_insert(self, session, execution_manager):
        client.post(
            '/task',
            json={
                'command': 'echo 65s',
                'trigger_type': 'interval',
                'trigger_args': {
                    'seconds': 5,
                    'minutes': 1
                }
            }
        )
        executor = execution_manager.task_executors[1]
        assert executor.task == IntervalTask('echo 65s', seconds=5, minutes=1)

    async def test_delete(self, session, add_one_task, execution_manager):
        assert len(execution_manager.task_executors) == 1
        client.delete('/task/1')
        assert len(execution_manager.task_executors) == 0

    async def test_update(self, session, add_one_task, execution_manager):
        old_task = execution_manager.task_executors[1].task
        assert old_task == IntervalTask('echo 0.25s', seconds=0.25)

        client.post(
            '/task/1',
            json={
                'command': 'echo 65s',
                'trigger_type': 'interval',
                'trigger_args': {
                    'seconds': 5,
                    'minutes': 1
                }
            }
        )
        new_task = execution_manager.task_executors[1].task
        assert new_task == IntervalTask('echo 65s', seconds=5, minutes=1)


class TestExecution:
    async def test_never_launched(self, session, add_one_task, execution_manager):
        executor = execution_manager.task_executors[1]

        exec_logs = (await session.scalars(select(ExecutionLog))).all()
        assert not executor.active and executor.status == 'never launched' and len(exec_logs) == 0

    async def test_active_after_run(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        executor = execution_manager.task_executors[1]
        assert executor.active

    async def test_run(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        executor = execution_manager.task_executors[1]
        assert executor.task == IntervalTask('echo 0.25s', seconds=0.25)

        await asyncio.sleep(0.3)
        await logger.flush()

        exec_out_logs = (await session.scalars(select(ExecutionOutputLog))).all()
        assert len(exec_out_logs) > 0

    async def test_running(self, event_loop, session, add_one_task, execution_manager):
        session.add(IntervalTask('echo start & timeout 1 > NUL', seconds=1))
        session.commit()
        await execution_manager.sync()
        client.post('/run_executor/1')

        await asyncio.sleep(0.5)
        await logger.flush()

        assert execution_manager.task_executors[1].status == 'started'

    async def test_inactive_after_run_and_stop(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        client.post('/stop_executor/1')
        executor = execution_manager.task_executors[1]
        assert not executor.active

    async def test_stop(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        await asyncio.sleep(0.3)

        client.post('/stop_executor/1')
        await asyncio.sleep(0.3)

        await logger.flush()
        exec_logs = (await session.scalars(select(ExecutionLog))).all()
        assert len(exec_logs) == 1
        assert all(log.status == 'finished' for log in exec_logs)
