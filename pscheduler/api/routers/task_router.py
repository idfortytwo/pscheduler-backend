from fastapi import HTTPException, Depends

from api.models import TaskInputModel
from api.routers._shared import router, TaskNotFound
from db.dal import DAL, get_dal


@router.get('/task', status_code=200)
async def get_tasks(db: DAL = Depends(get_dal)):
    tasks = await db.get_tasks()
    return {'tasks': [
        task.to_dict()
        for task
        in tasks
    ]}


@router.get('/task/{task_id}', status_code=200)
async def get_task(task_id: int, db: DAL = Depends(get_dal)):
    task = await db.get_task(task_id)
    if task:
        return {'task': task.to_dict()}
    else:
        raise TaskNotFound(task_id)


@router.post('/task', status_code=201)
async def add_task(task: TaskInputModel, db: DAL = Depends(get_dal)):
    try:
        new_task = await db.add_task(task)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {'task_id': new_task.task_id}


@router.delete('/task/{task_id}', status_code=200)
async def delete_task(task_id: int, db: DAL = Depends(get_dal)):
    await db.delete_task(task_id)


@router.post('/task/{task_id}', status_code=200)
async def update_task(task_id: int, task: TaskInputModel, db: DAL = Depends(get_dal)):
    await db.update_task(task_id, task)
