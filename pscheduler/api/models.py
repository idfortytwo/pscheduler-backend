from pydantic import BaseModel


class TaskInputModel(BaseModel):
    command_args: str
    trigger_type: str
    trigger_args: dict