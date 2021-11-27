from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from starlette.middleware.cors import CORSMiddleware
from uvicorn import Server, Config

from db.connection import session_scope, Session
from scheduler.executor import TaskManager
from scheduler.taskconfig import TaskConfig, TaskConfigFactory


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        return str(executor)
    else:
        return 'not found'


@app.get('/task_config')
async def get_task_configs():
    async with Session() as session:
        task_configs = await session.execute(select(TaskConfig))
        return [
            task.to_dict()
            for task
            in task_configs.scalars()
        ]


@app.get('/task_config/{task_config_id}')
async def get_task_config(task_config_id: int):
    async with session_scope() as session:
        stmt = select(TaskConfig).filter(TaskConfig.task_config_id == task_config_id)
        task_config = (await session.execute(stmt)).scalar()
        if task_config:
            return task_config.to_dict()
        else:
            return 'not found'


class TaskConfigInputModel(BaseModel):
    command_args: str
    trigger_type: str
    trigger_args: dict


@app.post('/task_config', status_code=201)
async def add_task_config(task_config: TaskConfigInputModel):
    try:
        new_task = TaskConfigFactory.create(**task_config.dict())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with Session() as session:
        session.add(new_task)
        await session.commit()
        await session.refresh(new_task)

    task_manager.add_task(new_task)
    task_manager.run_all()

    return task_config