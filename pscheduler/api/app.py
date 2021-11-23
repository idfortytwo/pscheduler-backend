from fastapi import FastAPI

from scheduler.executor import task_manager

app = FastAPI()


@app.get('/')
async def read_root():
    return {
        k: str(v)
        for k, v
        in task_manager.task_dict.items()
    }
