import asyncio
import collections
from typing import Deque

from db.connection import Session
from db.models import TaskOutputLog
from util.singleton import SingletonMeta


class TaskOutputLogger(metaclass=SingletonMeta):
    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._buffer: Deque[TaskOutputLog] = collections.deque()

        self._loop.create_task(self._flush_periodically())

    def log(self, value: TaskOutputLog):
        self._buffer.append(value)

    async def _flush_periodically(self, seconds=1):
        while True:
            await self.flush()
            await asyncio.sleep(seconds)

    async def flush(self):
        async with Session() as session:
            logs = [self._buffer.pop() for _ in range(len(self._buffer))]
            session.add_all(logs)
            await session.commit()