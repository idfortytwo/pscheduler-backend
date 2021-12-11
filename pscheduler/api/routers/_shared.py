from fastapi import APIRouter, HTTPException

from scheduler.executor import ExecutionManager


router = APIRouter()
execution_manager = ExecutionManager()


def TaskNotFound(task_id: int):
    return HTTPException(status_code=404, detail=f"No task with ID {task_id}")