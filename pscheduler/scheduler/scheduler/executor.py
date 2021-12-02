import asyncio
import sqlalchemy

from asyncio import TimerHandle
from datetime import datetime, timedelta
from typing import List, Dict, Union

from db.connection import Session
from scheduler.task import Task


class TaskExecutor:
    def __init__(self, task: Task):
        self._task = task
        self._loop = asyncio.get_event_loop()
        self._timer_handle: Union[TimerHandle, None] = None
        self._is_running = False

    # @property
    # def task(self):
    #     return self._task

    @property
    def is_running(self):
        return self._is_running

    def run(self):
        if not self._is_running:
            self._next_run_date_iter = self._task.get_next_run_date_iter()
            self._timer_handle = self._loop.call_at(self._get_next_run_ts(),
                                                    lambda: asyncio.ensure_future(self._run_iteration()))
            self._is_running = True

    def stop(self):
        self._task.reset_iter()
        if self._timer_handle:
            self._timer_handle.cancel()
        self._is_running = False

    def __del__(self):
        self.stop()

    def _get_next_run_ts(self) -> float:
        run_date = next(self._next_run_date_iter)
        loop_base_time = datetime.utcnow() - timedelta(seconds=self._loop.time())
        return (run_date - loop_base_time).total_seconds()

    async def _run_iteration(self):
        self._timer_handle = self._loop.call_at(self._get_next_run_ts(),
                                                lambda: asyncio.ensure_future(self._run_iteration()))

        print(f'running at {datetime.utcnow()}')
        return_code = await self._execute_process()
        print(f'finished with code {return_code}\n')

    async def _execute_process(self) -> int:
        sub: asyncio.subprocess.Process = await asyncio.create_subprocess_shell(
            self._task.command,
            stdout=asyncio.subprocess.PIPE,
            shell=True)

        while line := await sub.stdout.readline():
            print(line.decode(), end='')

        return sub.returncode

    def to_dict(self):
        return {
            'task': self._task.to_dict(),
            'is_running': self.is_running
        }

    def __str__(self):
        return f"TaskExecutor('{self._task.command}', {self._task.trigger_type}, " \
               f"'{self._task.trigger_args}')"


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class TaskManager(metaclass=SingletonMeta):
    def __init__(self):
        self.tasks: Dict[int, TaskExecutor] = {}

    async def sync(self):
        async with Session() as session:
            select_stmt = sqlalchemy.select(Task)
            tasks_rs = await session.execute(select_stmt)
            db_tasks: List[Task] = list(tasks_rs.scalars())

            self._append_db_tasks(db_tasks)
            self._delete_db_tasks(db_tasks)

    def _append_db_tasks(self, db_tasks: List[Task]):
        for db_task in db_tasks:
            if db_task.task_id not in self.tasks:
                self.tasks.update({db_task.task_id: TaskExecutor(db_task)})

    def _delete_db_tasks(self, db_tasks):
        db_task_ids = set(db_task.task_id for db_task in db_tasks)
        curr_task_ids = set(self.tasks.keys())
        redundant_task_ids = curr_task_ids - db_task_ids
        for task_id in redundant_task_ids:
            del self.tasks[task_id]

    def run_task(self, task_id: int):
        self.tasks[task_id].run()

    def run_all(self):
        for task_id in self.tasks.keys():
            self.run_task(task_id)

    def stop_task(self, task_id: int):
        self.tasks[task_id].stop()

    def stop_all(self):
        for task_id in self.tasks.keys():
            self.stop_task(task_id)