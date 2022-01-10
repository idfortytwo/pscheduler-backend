import asyncio
import datetime
import json
from typing import List

import pytest
from sqlalchemy import select, insert
from db.models import ProcessLog, StdoutLog, OutputLog, StderrLog
from scheduler.executor import ExecutionManager, ExecutionMonitor
from scheduler.task import IntervalTask, Task, DateTask
from tests.testing import event_loop, client, session, add_one_task, add_long_task, add_three_tasks, setup_db  # noqa
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
        executor = execution_manager.task_executors.get(1)
        if executor:
            print(executor.task)
        assert len(execution_manager.task_executors) == 0

    async def test_insert(self, session, execution_manager):
        client.post(
            '/task',
            json={
                'title': 'every 65s',
                'descr': None,
                'command': 'echo 65s',
                'trigger_type': 'interval',
                'trigger_args': {
                    'seconds': 5,
                    'minutes': 1
                }
            }
        )
        executor = execution_manager.task_executors[1]
        assert executor.task == IntervalTask('every 65s', 'echo 65s', seconds=5, minutes=1)

    async def test_delete(self, session, add_one_task, execution_manager):
        assert len(execution_manager.task_executors) == 1
        client.delete('/task/1')
        assert len(execution_manager.task_executors) == 0

    async def test_update(self, session, add_one_task, execution_manager):
        old_task = execution_manager.task_executors[1].task
        assert old_task == IntervalTask('every 0.25s', 'echo 0.25s', seconds=0.25)

        client.post(
            '/task/1',
            json={
                'title': 'every 65 second',
                'descr': None,
                'command': 'echo 65s',
                'trigger_type': 'interval',
                'trigger_args': {
                    'seconds': 5,
                    'minutes': 1
                }
            }
        )
        new_task = execution_manager.task_executors[1].task
        assert new_task == IntervalTask('every 65s', 'echo 65s', seconds=5, minutes=1)


class TestExecution:
    async def test_never_launched(self, session, add_one_task, execution_manager):
        executor = execution_manager.task_executors[1]

        exec_logs = (await session.scalars(select(ProcessLog))).all()
        assert not executor.active and executor.status == 'never launched' and len(exec_logs) == 0

    async def test_active_after_run(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        executor = execution_manager.task_executors[1]
        assert executor.active

    async def test_run(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        executor = execution_manager.task_executors[1]
        assert executor.task == IntervalTask('every 0.25s', 'echo 0.25s', seconds=0.25)

        await asyncio.sleep(0.4)
        await logger.flush()

        exec_out_logs = (await session.scalars(select(StdoutLog))).all()
        assert len(exec_out_logs) > 0

    async def test_running(self, event_loop, session, add_long_task, execution_manager):
        await execution_manager.sync()
        client.post('/run_executor/1')

        await asyncio.sleep(0.3)
        await logger.flush()

        assert execution_manager.task_executors[1].status == 'started'

    async def test_inactive_after_run_and_stop(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        client.post('/stop_executor/1')
        executor = execution_manager.task_executors[1]
        assert not executor.active

    async def test_stop(self, event_loop, session, add_one_task, execution_manager):
        client.post('/run_executor/1')
        await asyncio.sleep(0.2)

        client.post('/stop_executor/1')
        await asyncio.sleep(0.2)

        await logger.flush()
        exec_logs = (await session.scalars(select(ProcessLog))).all()
        assert len(exec_logs) == 1
        assert all(log.status == 'finished' for log in exec_logs)

    async def test_get_all(self, event_loop, session, add_three_tasks, execution_manager):
        resp = client.get('/executor')

        tasks = (await session.scalars(select(Task))).all()
        assert json.loads(resp.content) == {
            'task_executors': [
                {
                    'task': task.to_dict(),
                    'active': False,
                    'status': 'never launched'
                }
                for task in tasks
            ]
        }

    @staticmethod
    @pytest.fixture
    async def mixed_output_executor(session) -> ExecutionMonitor:
        task = IntervalTask('run mixed script', 'python script.py 0.01', seconds=1)
        task.task_id = 1
        return ExecutionMonitor(task, lambda _: None)

    @pytest.fixture
    def mixed_output_error_order(self):
        return [0, 1, 0, 0, 1, 0, 0, 0, 1]

    async def test_output_logs_error_status(self, session,
                                            mixed_output_executor: ExecutionMonitor, mixed_output_error_order):
        await mixed_output_executor.start()
        logs: List[OutputLog] = await session.scalars(
            select(OutputLog)
        )

        for log, is_error in zip(logs, mixed_output_error_order):
            assert log.is_error == is_error

    async def test_output_logs_polymorphism(self, session,
                                            mixed_output_executor: ExecutionMonitor, mixed_output_error_order):
        await mixed_output_executor.start()
        logs: List[OutputLog] = await session.scalars(
            select(OutputLog)
        )

        for log, is_error in zip(logs, mixed_output_error_order):
            assert log.__class__ == [StdoutLog, StderrLog][is_error]