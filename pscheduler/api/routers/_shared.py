from fastapi import APIRouter

from scheduler.executor import TaskManager


router = APIRouter()
task_manager = TaskManager()