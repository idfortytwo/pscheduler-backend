import asyncio
import sqlalchemy

from asyncio import TimerHandle
from asyncio.subprocess import Process
from datetime import datetime
from aiostream import stream
from typing import List, Dict, Union, Callable, AsyncGenerator

from db.connection import Session
from db.models import ProcessLog, ExecutionState, StdoutLog, StderrLog
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
            self._active = True

            self._loop.create_task(self._scheduling_loop())

    async def _scheduling_loop(self):
        for run_date in RunDateIterator(self.task):
            if not self._active:
                break
            await self._await_run(run_date)

            self._current_execution = ExecutionMonitor(self.task, status_callback=self._update_status)
            self._loop.create_task(self._current_execution.start())

        self._active = False

    @staticmethod
    async def _await_run(run_date: datetime):
        delay = run_date - datetime.now()
        await asyncio.sleep(delay.total_seconds())

    def stop(self):
        self._active = False

    def _update_status(self, status):
        self.status = status

    def to_dict(self):
        return {
            'task': self._task.to_dict(),
            'active': self.active,
            'status': self.status
        }

    def __str__(self):
        return f"TaskExecutor('{self._task.command}', {self._task.trigger_type}, '{self._task.trigger_args}')"


class RunDateIterator:
    def __init__(self, task: Task):
        self._task = task
        self._loop = asyncio.get_event_loop()

    def __iter__(self):
        self._run_date_iter = iter(self._task.run_date_iter)
        return self

    def __next__(self) -> datetime:
        run_date = self._get_next_run_date()
        if not run_date:
            raise StopIteration
        return run_date

    def _get_next_run_date(self) -> Union[datetime, None]:
        run_date = next(self._run_date_iter)
        return self._skip_missed(run_date)

    def _skip_missed(self, run_date: datetime) -> datetime:
        missed = False

        while run_date < datetime.now():
            missed = True
            self._log_missed_run(run_date)
            run_date = next(self._run_date_iter)

        if missed:
            self._log_missed_run(run_date)

        return run_date

    def _log_missed_run(self, run_date: datetime):
        missed_log = ProcessLog(self._task.task_id, start_date=run_date)
        missed_log.set_state(ExecutionState.MISSED)
        logger.log(missed_log)


class ExecutionMonitor:
    def __init__(self, task: Task, status_callback: Callable):
        self._task = task
        self._status_callback = status_callback
        self._log = ProcessLog(self._task.task_id)

    async def start(self):
        await self._log_start()

        return_code = await self._execute_process()
        return return_code

    async def _log_start(self):
        async with Session(expire_on_commit=False) as session:
            self._log = ProcessLog(self._task.task_id)
            self._log.set_state(ExecutionState.STARTED)
            self._status_callback(self._log.status)

            session.add(self._log)
            await session.commit()

    async def _execute_process(self) -> int:
        process = await asyncio.create_subprocess_shell(
            self._task.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            shell=True)

        async for output_log in self._yield_output_logs(process):
            print(output_log.message, end='')
            logger.log(output_log)

        await process.wait()
        return_code = process.returncode
        await self._log_end(return_code)
        return return_code

    async def _yield_stdout_logs(self, process: Process):
        while line := await process.stdout.readline():
            yield StdoutLog(line.decode(), datetime.utcnow(), self._log.process_log_id)

    async def _yield_stderr_logs(self, process: Process):
        while line := await process.stderr.readline():
            yield StderrLog(line.decode(), datetime.utcnow(), self._log.process_log_id)

    async def _yield_output_logs(self, process: Process) -> AsyncGenerator[Union[StdoutLog, StderrLog], None]:
        output = stream.merge(self._yield_stdout_logs(process), self._yield_stderr_logs(process))
        async with output.stream() as streamer:
            async for log in streamer:
                yield log

    async def _log_end(self, return_code: int):
        if return_code:
            await self._log_failed(return_code)
        else:
            await self._log_finish()
        await logger.flush()

    async def _log_finish(self):
        async with Session(expire_on_commit=False) as session:
            self._log.set_state(ExecutionState.FINISHED)
            self._log.finish_date = datetime.utcnow()
            self._status_callback(self._log.status)

            session.add(self._log)
            await session.commit()

    async def _log_failed(self, return_code: int):
        async with Session(expire_on_commit=False) as session:
            self._log.return_code = return_code
            self._log.set_state(ExecutionState.FAILED)
            self._log.finish_date = datetime.utcnow()
            self._status_callback(self._log.status)

            session.add(self._log)
            await session.commit()


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