import asyncio

from api.app import server
from scheduler.executor import ExecutionManager
from db.connection import engine
from db.models import Base


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_task_manager = ExecutionManager()
    await async_task_manager.sync()
    async_task_manager.run_all()

    await server.serve()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    loop.run_until_complete(task)
