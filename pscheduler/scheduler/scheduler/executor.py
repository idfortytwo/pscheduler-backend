import asyncio
from asyncio import TimerHandle
from datetime import datetime, timedelta
from typing import List, Dict

from scheduler.taskconfig import TaskConfig


class TaskExecutor:
    def __init__(self, task_config: TaskConfig):
        self._task_config = task_config
        self._loop = asyncio.get_event_loop()
        self._next_run_date_it = iter(task_config.get_next_run_date_it())
        self._timer_handle: TimerHandle

    def run(self):
        self._timer_handle = self._loop.call_at(self._get_next_run_ts(),
                                                lambda: asyncio.ensure_future(self._run_iteration()))

    def stop(self):
        if self._timer_handle:
            self._timer_handle.cancel()

    def _get_next_run_ts(self):
        run_date = next(self._next_run_date_it)
        loop_base_time = datetime.utcnow() - timedelta(seconds=self._loop.time())
        return (run_date - loop_base_time).total_seconds()

    async def _run_iteration(self):
        self._timer_handle = self._loop.call_at(self._get_next_run_ts(),
                                                lambda: asyncio.ensure_future(self._run_iteration()))

        print(f'running at {datetime.utcnow()}')
        return_code = await self._execute_process()
        print(f'finished with code {return_code}\n')

    async def _execute_process(self):
        commandargs = self._task_config.command_args
        sub: asyncio.subprocess.Process = await asyncio.create_subprocess_shell(
            commandargs,
            stdout=asyncio.subprocess.PIPE,
            shell=True)

        while line := await sub.stdout.readline():
            print(line.decode(), end='')

        return sub.returncode


class TaskManager:
    def __init__(self, task_configs: List[TaskConfig]):
        self._tasks_dict: Dict[int, TaskExecutor] = {
            task_config.task_config_id: TaskExecutor(task_config)
            for task_config
            in task_configs
        }
        for k, v in self._tasks_dict.items():
            print(k, v)

    @property
    def task_dict(self) -> Dict[int, TaskExecutor]:
        return self._tasks_dict

    def add_task(self, task_config: TaskConfig):
        self._tasks_dict.update({
            task_config.task_config_id: TaskExecutor(task_config)
        })

    def run_task(self, task_config_id: int):
        self._tasks_dict[task_config_id].run()

    def run_all(self):
        for task_config_id in self._tasks_dict.keys():
            self.run_task(task_config_id)

    def stop_task(self, task_config_id: int):
        self._tasks_dict[task_config_id].stop()

    def stop_all(self):
        for task_config_id in self._tasks_dict.keys():
            self.stop_task(task_config_id)
