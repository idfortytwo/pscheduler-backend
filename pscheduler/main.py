import asyncio

from uvicorn import Config, Server

from api.app import app
from db.prep import reset_data
from scheduler.executor import task_manager
from scheduler.taskconfig import TaskConfigs


async def main():
    await reset_data()
    configs = await TaskConfigs.fetch()
    task_manager.add_tasks(configs)
    task_manager.run_all()

    config = Config(app=app, host='127.0.0.1', port=8000, loop='asyncio')
    server = Server(config)
    await server.serve()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())

    loop.run_until_complete(task)
