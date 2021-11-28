from api.routers._shared import router, task_manager


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
        return 'not found'
