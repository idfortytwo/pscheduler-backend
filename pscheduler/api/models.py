from typing import Any, Optional, Dict, Union

from pydantic import BaseModel


class TaskInputModel(BaseModel):
    task_id: Optional[int]
    command: str
    trigger_type: str
    trigger_args: Union[str, Dict]