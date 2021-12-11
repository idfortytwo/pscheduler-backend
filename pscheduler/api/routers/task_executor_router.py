from api.routers._shared import router, execution_manager, TaskNotFound


@router.get('/executor', status_code=200)
async def get_executors():
    return {'task_executors': [
        executor.to_dict()
        for task_id, executor
        in execution_manager.task_executors.items()
    ]}


@router.post('/run_executor/{task_id}', status_code=200)
async def run_executor(task_id: int):
    try:
        execution_manager.run_task(task_id)
        return {'task_id': task_id}
    except KeyError:
        raise TaskNotFound(task_id)


@router.post('/stop_executor/{task_id}', status_code=200)
async def stop_executor(task_id: int):
    try:
        execution_manager.stop_task(task_id)
        return {'task_id': task_id}
    except KeyError:
        raise TaskNotFound(task_id)