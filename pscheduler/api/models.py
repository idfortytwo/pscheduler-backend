from typing import Optional, Dict, Union

from pydantic import BaseModel


class TaskInputModel(BaseModel):
    task_id: Optional[int]
    title: str
    descr: Union[str, None]
    command: str
    trigger_type: str
    trigger_args: Union[str, Dict]