import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.app import server
from db.connection import Session
from db.prep import reset_data
from scheduler.executor import TaskManager
from scheduler.task import Task


async def main():
    await reset_data()

    async with Session() as session:
        session: AsyncSession
        tasks_rs = await session.execute(select(Task))

    task_manager = TaskManager()
    task_manager.add_tasks(tasks_rs.scalars())
    task_manager.run_all()

    await server.serve()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    loop.run_until_complete(task)
