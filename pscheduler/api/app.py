from fastapi import FastAPI
from uvicorn import Server, Config

from scheduler.executor import TaskManager

app = FastAPI()
config = Config(app=app, host='127.0.0.1', port=8000, loop='asyncio')
server = Server(config)

task_manager = TaskManager()


@app.get('/executor')
async def get_executors():
    return {
        k: str(v)
        for k, v
        in task_manager.task_dict.items()
    }


@app.get('/executor/{task_config_id}')
async def get_executor(task_config_id: int):
    executor = task_manager.task_dict.get(task_config_id)
    if executor:
        return executor
    else:
        return 'not found'