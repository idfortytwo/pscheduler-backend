import asyncio

from api.app import server
from db.prep import reset_data
from scheduler.executor import TaskManager
from scheduler.taskconfig import TaskConfigs


async def main():
    await reset_data()
    configs = await TaskConfigs.fetch()

    task_manager = TaskManager()
    task_manager.add_tasks(configs)
    task_manager.run_all()

    await server.serve()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    loop.run_until_complete(task)
