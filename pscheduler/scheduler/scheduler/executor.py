import asyncio
from asyncio import TimerHandle
from datetime import datetime, timedelta
from typing import List, Dict, Union, Callable

import sqlalchemy
import sqlalchemy.event

from db.connection import Session
from db.models import ExecutionLog, ExecutionState, ExecutionOutputLog
from scheduler.task import Task
from util import SingletonMeta, logger


class TaskExecutor:
    def __init__(self, task: Task):
        self._task = task
        self._loop = asyncio.get_event_loop()
        self._timer_handle: Union[TimerHandle, None] = None
        self._active = False

        self.status = 'never launched'

    @property
    def task(self):
        return self._task

    @property
    def active(self):
        return self._active

    def run(self):
        if not self._active:
            self._run_date_iter = self._task.run_date_iter
            self._timer_handle = self._sched_next_run()
            self._active = True

    def stop(self):
        if self._timer_handle:
            self._timer_handle.cancel()
        self._active = False

    def _sched_next_run(self) -> TimerHandle:
        return self._loop.call_at(self._get_next_run_ts(),
                                  lambda: asyncio.ensure_future(self._run_iteration()))

    def _log_missed(self, run_date: datetime):
        missed_log = ExecutionLog(self._task.task_id, start_date=run_date)
        missed_log.set_state(ExecutionState.MISSED)
        logger.log(missed_log)

    def _get_next_run_ts(self) -> float:
        loop_base_time = datetime.utcnow() - timedelta(seconds=self._loop.time())
        now_ts = (datetime.utcnow() - loop_base_time).total_seconds()

        run_date = next(self._run_date_iter)

        missed = False
        while (next_run_ts := (run_date - loop_base_time).total_seconds()) - now_ts < 0:
            missed = True
            self._log_missed(run_date)
            run_date = next(self._run_date_iter)

        if missed:
            self._log_missed(run_date)

        return next_run_ts

    async def _run_iteration(self):
        self._timer_handle = self._sched_next_run()

        self._current_execution = Execution(self.task, status_callback=self.update_status)
        await self._current_execution.start()

    def update_status(self, status):
        self.status = status

    def to_dict(self):
        return {
            'task': self._task.to_dict(),
            'active': self.active,
            'status': self.status
        }

    def __str__(self):
        return f"TaskExecutor('{self._task.command}', {self._task.trigger_type}, " \
               f"'{self._task.trigger_args}')"


class Execution:
    def __init__(self, task: Task, status_callback: Callable):
        self._task = task
        self._status_callback = status_callback
        self._log = ExecutionLog(self._task.task_id)

    async def start(self):
        await self._log_start()

        return_code = await self._execute_process()
        return return_code

    async def _execute_process(self) -> int:
        sub: asyncio.subprocess.Process = await asyncio.create_subprocess_shell(
            self._task.command,
            stdout=asyncio.subprocess.PIPE,
            shell=True)

        while line := await sub.stdout.readline():
            print(line.decode(), end='')
            line_log = ExecutionOutputLog(line.decode(), datetime.utcnow(), self._log.execution_log_id)
            logger.log(line_log)

        await self._log_finish()

        return sub.returncode

    async def _log_start(self):
        async with Session(expire_on_commit=False) as session:
            self._log = ExecutionLog(self._task.task_id)
            self._log.set_state(ExecutionState.STARTED)
            self._status_callback(self._log.status)

            session.add(self._log)
            await session.commit()

    async def _log_finish(self):
        async with Session(expire_on_commit=False) as session:
            self._log.set_state(ExecutionState.FINISHED)
            self._status_callback(self._log.status)
            self._log.finish_date = datetime.utcnow()

            session.add(self._log)
            await session.commit()

    @property
    def status(self):
        return self._log.status


class ExecutionManager(metaclass=SingletonMeta):
    def __init__(self):
        self.task_executors: Dict[int, TaskExecutor] = {}
        self._loop = asyncio.get_event_loop()

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
        if current_executor.active:
            new_executor.run()

        current_executor.stop()
        del current_executor

    def _delete_db_tasks(self, db_tasks: List[Task]):
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