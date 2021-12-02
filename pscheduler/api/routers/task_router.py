import sqlalchemy

from fastapi import HTTPException
from sqlalchemy import select

from api.models import TaskInputModel
from api.routers._shared import router, task_manager, TaskNotFound
from db.connection import Session, session_scope
from scheduler.task import Task, TaskFactory


@router.get('/task', status_code=200)
async def get_tasks():
    async with Session() as session:
        tasks = await session.execute(select(Task))
        return {'tasks': [
            task.to_dict()
            for task
            in tasks.scalars()
        ]}


@router.get('/task/{task_id}', status_code=200)
async def get_task_config(task_id: int):
    async with session_scope() as session:
        stmt = select(Task).filter(Task.task_id == task_id)
        task = (await session.execute(stmt)).scalar()
        if task:
            return {'task': task.to_dict()}
        else:
            raise TaskNotFound(task_id)


@router.post('/task', status_code=201)
async def add_task(task: TaskInputModel):
    try:
        new_task = TaskFactory.create(task.command, task.trigger_type, task.trigger_args)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with Session() as session:
        session.add(new_task)
        await session.commit()
        await session.refresh(new_task)

    await task_manager.sync()
    task_manager.run_task(new_task.task_id)

    return {'task_id': new_task.task_id}


@router.delete('/task/{task_id}', status_code=200)
async def delete_task(task_id: int):
    async with Session() as session:
        select_stmt = sqlalchemy.select(Task).filter(Task.task_id == task_id)
        task_to_delete = (await session.execute(select_stmt)).scalar()

        if task_to_delete:
            await session.delete(task_to_delete)
            await session.commit()
        else:
            raise TaskNotFound(task_id)

        await task_manager.sync()
        return {'task_id': task_id}


@router.post('/task/{task_id}', status_code=200)
async def update_task(task_id: int, task: TaskInputModel):
    async with Session() as session:
        select_stmt = sqlalchemy.select(Task).filter(Task.task_id == task_id)
        task_to_delete = (await session.execute(select_stmt)).scalar()
        if task_to_delete:
            await session.delete(task_to_delete)
            await session.commit()

    await task_manager.sync()

    # async with Session() as session:
    #     update_stmt = sqlalchemy.update(Task).filter(Task.task_id == task_id)
    #     update_stmt = update_stmt.values(trigger_args=str(updated_task_data.trigger_args))
    #     update_stmt = update_stmt.values(command=updated_task_data.command)
    #     update_stmt = update_stmt.values(trigger_type=updated_task_data.trigger_type)
    #     await session.execute(update_stmt)
    #     await session.commit()

    try:
        new_task = TaskFactory.create(task.command, task.trigger_type, task.trigger_args)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with Session() as session:
        session.add(new_task)
        await session.commit()
        await session.refresh(new_task)

    await task_manager.sync()
    task_manager.run_task(new_task.task_id)

    return {'task_id': task_id}
