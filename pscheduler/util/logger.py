import asyncio
import collections
from typing import Deque

from db.connection import Session
from db.models import ExecutionOutputLog
from util.singleton import SingletonMeta


class TaskOutputLogger(metaclass=SingletonMeta):
    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._buffer: Deque[ExecutionOutputLog] = collections.deque()

        self._loop.create_task(self._flush_periodically())

    def log(self, record: ExecutionOutputLog):
        self._buffer.append(record)

    async def _flush_periodically(self, seconds=1):
        while True:
            await self.flush()
            await asyncio.sleep(seconds)

    async def flush(self):
        async with Session() as session:
            logs = [self._buffer.popleft() for _ in range(len(self._buffer))]
            session.add_all(logs)
            await session.commit()