from fastapi import APIRouter, HTTPException

from scheduler.executor import TaskManager


router = APIRouter()
task_manager = TaskManager()


def TaskNotFound(task_id: int):
    return HTTPException(status_code=404, detail=f"No task with ID {task_id}")