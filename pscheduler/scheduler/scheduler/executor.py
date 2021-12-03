import asyncio
from asyncio import TimerHandle
from datetime import datetime, timedelta
from typing import List, Dict, Union

import sqlalchemy
import sqlalchemy.event

from db.connection import Session
from db.models import TaskRunLog, ExecutionState
from scheduler.task import Task


class TaskExecutor:
    def __init__(self, task: Task):
        self._task = task
        self._loop = asyncio.get_event_loop()
        self._timer_handle: Union[TimerHandle, None] = None
        self._is_running = False

        self._current_execution: Union[Execution, None] = None

    @property
    def task(self):
        return self._task

    @property
    def is_running(self):
        return self._is_running

    def run(self):
        if not self._is_running:
            self._next_run_date_iter = self._task.get_next_run_date_iter()
            self._timer_handle = self._sched_next_run()
            self._is_running = True

    def stop(self):
        self._task.reset_iter()
        if self._timer_handle:
            self._timer_handle.cancel()
        self._is_running = False

    def _sched_next_run(self) -> TimerHandle:
        return self._loop.call_at(self._get_next_run_ts(),
                                  lambda: asyncio.ensure_future(self._run_iteration()))

    def _get_next_run_ts(self) -> float:
        run_date = next(self._next_run_date_iter)
        loop_base_time = datetime.utcnow() - timedelta(seconds=self._loop.time())
        return (run_date - loop_base_time).total_seconds()

    async def _run_iteration(self):
        self._timer_handle = self._sched_next_run()

        self._current_execution = Execution(self.task)
        await self._current_execution.start()

    def to_dict(self):
        return {
            'task': self._task.to_dict(),
            'is_running': self.is_running,
            'status': self.get_status()
        }

    def get_status(self):
        if self._current_execution:
            return self._current_execution.status
        else:
            return ExecutionState.AWAITING.name.lower()

    def __str__(self):
        return f"TaskExecutor('{self._task.command}', {self._task.trigger_type}, " \
               f"'{self._task.trigger_args}')"


class Execution:
    def __init__(self, task: Task):
        self._task = task
        self.log = TaskRunLog(self._task.task_id)

    async def start(self):
        self._log_start()

        return_code = await self._execute_process()
        return return_code

    async def _execute_process(self) -> int:
        sub: asyncio.subprocess.Process = await asyncio.create_subprocess_shell(
            self._task.command,
            stdout=asyncio.subprocess.PIPE,
            shell=True)

        while line := await sub.stdout.readline():
            print(line.decode(), end='')

        self._log_finish()

        return sub.returncode

    def _log_start(self):
        async with Session(expire_on_commit=False) as session:
            self.log = TaskRunLog(self._task.task_id)
            self.log.set_state(ExecutionState.STARTED)
            session.add(self.log)
            await session.commit()

    def _log_finish(self):
        async with Session(expire_on_commit=False) as session:
            self.log.set_state(ExecutionState.FINISHED)
            self.log.finish_date = datetime.utcnow()
            session.add(self.log)
            await session.commit()

    @property
    def status(self):
        return self.log.status


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class TaskManager(metaclass=SingletonMeta):
    def __init__(self):
        self.task_executors: Dict[int, TaskExecutor] = {}
        self._loop = asyncio.get_event_loop()

    def enable_listening(self, engine):
        sqlalchemy.event.listens_for(engine.sync_engine, 'commit')(self.on_commit)

    def on_commit(self, conn):  # noqa
        if not self._loop:
            self._loop = asyncio.get_event_loop()

        self._loop.create_task(self.sync())

    async def sync(self):
        async with Session() as session:
            select_stmt = sqlalchemy.select(Task)
            tasks_rs = await session.execute(select_stmt)
            db_tasks: List[Task] = list(tasks_rs.scalars())

            self._update_db_tasks(db_tasks)
            self._delete_db_tasks(db_tasks)

    def _update_db_tasks(self, db_tasks: List[Task]):
        for db_task in db_tasks:
            if db_task.task_id in self.task_executors:
                current_executor = self.task_executors[db_task.task_id]
                if db_task != current_executor.task:
                    self._update_task(current_executor, db_task)
            else:
                self._add_task(db_task)

    def _add_task(self, new_task: Task):
        self.task_executors.update({new_task.task_id: TaskExecutor(new_task)})

    def _update_task(self, current_executor: TaskExecutor, new_task: Task):
        new_executor = TaskExecutor(new_task)
        self.task_executors.update({new_task.task_id: new_executor})
        if current_executor.is_running:
            new_executor.run()

        current_executor.stop()
        del current_executor

    def _delete_db_tasks(self, db_tasks):
        db_task_ids = set(db_task.task_id for db_task in db_tasks)
        curr_task_ids = set(self.task_executors.keys())
        for task_id in curr_task_ids - db_task_ids:
            self.task_executors[task_id].stop()
            del self.task_executors[task_id]

    def run_task(self, task_id: int):
        self.task_executors[task_id].run()

    def run_all(self):
        for task_id in self.task_executors.keys():
            self.run_task(task_id)

    def stop_task(self, task_id: int):
        self.task_executors[task_id].stop()

    def stop_all(self):
        for task_id in self.task_executors.keys():
            self.stop_task(task_id)