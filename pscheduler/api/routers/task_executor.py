from api.routers._shared import router, task_manager, TaskNotFound


@router.get('/executor')
async def get_executors():
    return {
        k: str(v)
        for k, v
        in task_manager.task_dict.items()
    }


@router.get('/executor/{task_config_id}')
async def get_executor(task_config_id: int):
    executor = task_manager.task_dict.get(task_config_id)
    if executor:
        return str(executor)
    else:
        raise TaskNotFound(task_config_id)


@router.delete('/executor/{task_config_id}', status_code=200)
async def delete_executor(task_config_id: int):
    try:
        task_manager.delete_task(task_config_id)
        return {'task_config_id': task_config_id}
    except KeyError:
        raise TaskNotFound(task_config_id)