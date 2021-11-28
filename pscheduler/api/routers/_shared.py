from fastapi import APIRouter, HTTPException

from scheduler.executor import TaskManager


router = APIRouter()
task_manager = TaskManager()


def TaskNotFound(task_config_id: int):
    return HTTPException(status_code=404, detail=f"No task config with ID {task_config_id}")