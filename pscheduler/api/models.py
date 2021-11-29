from pydantic import BaseModel


class TaskConfigModel(BaseModel):
    command_args: str
    trigger_type: str
    trigger_args: dict