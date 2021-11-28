import sqlalchemy

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import select

from api.routers._shared import router, task_manager
from db.connection import Session, session_scope
from scheduler.taskconfig import TaskConfig, TaskConfigFactory


@router.get('/task_config')
async def get_task_configs():
    async with Session() as session:
        task_configs = await session.execute(select(TaskConfig))
        return [
            task.to_dict()
            for task
            in task_configs.scalars()
        ]


@router.get('/task_config/{task_config_id}')
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


@router.post('/task_config', status_code=201)
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


@router.delete('/task_config/{task_config_id}', status_code=200)
async def delete_task_config(task_config_id: int):
    async with Session() as session:
        select_stmt = sqlalchemy.select(TaskConfig).filter(TaskConfig.task_config_id == task_config_id)
        task_to_delete = (await session.execute(select_stmt)).scalar()

        if task_to_delete:
            await session.delete(task_to_delete)
            await session.commit()
            return {
                'deleted': task_to_delete.to_dict()
            }
        else:
            raise HTTPException(status_code=404, detail=f"No task config with ID {task_config_id}")
