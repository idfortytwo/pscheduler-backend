from api.routers._shared import router, task_manager, TaskNotFound


@router.get('/executor', status_code=200)
async def get_executors():
    return {'task_executors': [
        executor.to_dict()
        for task_id, executor
        in task_manager.task_executors.items()
    ]}


@router.get('/executor/{task_id}', status_code=200)
async def get_executor(task_id: int):
    executor = task_manager.task_executors.get(task_id)
    if executor:
        return {'task_executor': executor.to_dict()}
    else:
        raise TaskNotFound(task_id)


@router.post('/run_executor/{task_id}', status_code=200)
async def run_executor(task_id: int):
    try:
        task_manager.run_task(task_id)
        return {'task_id': task_id}
    except KeyError:
        raise TaskNotFound(task_id)


@router.post('/stop_executor/{task_id}', status_code=200)
async def stop_executor(task_id: int):
    try:
        task_manager.stop_task(task_id)
        return {'task_id': task_id}
    except KeyError:
        raise TaskNotFound(task_id)