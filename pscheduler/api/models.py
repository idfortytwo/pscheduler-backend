from pydantic import BaseModel


class TaskInputModel(BaseModel):
    command: str
    trigger_type: str
    trigger_args: dict