import asyncio


from api.app import server
from db.prep import reset_data
from scheduler.executor import TaskManager


async def main():
    await reset_data()

    async_task_manager = TaskManager()
    await async_task_manager.sync()
    async_task_manager.run_all()

    await server.serve()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    loop.run_until_complete(task)
