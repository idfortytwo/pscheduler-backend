import ast

from datetime import datetime, timedelta
from abc import abstractmethod, ABC
from typing import Iterator, NoReturn

from db.models import TaskModel


class Task(TaskModel, ABC):
    @abstractmethod
    def __init__(self, command_args: str, schedule_params: any):
        self.command_args = command_args
        self.trigger_args = schedule_params

    @abstractmethod
    def get_next_run_date_iter(self) -> Iterator[datetime]:
        pass

    @abstractmethod
    def reset_iter(self) -> NoReturn:
        pass

    def to_dict(self):
        return {
            'task_id': self.task_id,
            'command_args': self.command_args,
            'trigger_type': self.trigger_type,
            'trigger_args': self.trigger_args
        }

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.command_args}\', {self.trigger_args})'


class CronTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'cron'}

    def __init__(self, command_args: str, schedule_params: any):
        super().__init__(command_args, schedule_params)

    def get_next_run_date_iter(self) -> Iterator[datetime]:
        pass

    def reset_iter(self) -> NoReturn:
        pass


class IntervalTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'interval'}

    def __init__(self, command_args: str, /, *,
                 days=0, seconds=0, minutes=0, hours=0, weeks=0):
        kwargs = {
            'days': days,
            'seconds': seconds,
            'minutes': minutes,
            'hours': hours,
            'weeks': weeks
        }
        trigger_args = {k: v for k, v in kwargs.items() if v}
        super().__init__(command_args, str(trigger_args))

    @property
    def interval(self):
        args = ast.literal_eval(self.trigger_args)
        return timedelta(**args)

    def reset_iter(self):
        if hasattr(self, 'run_date'):
            delattr(self, 'run_date')

    def get_next_run_date_iter(self) -> Iterator[datetime]:
        while True:
            if not hasattr(self, 'run_date'):
                self.run_date = datetime.utcnow() - self.interval
            self.run_date += self.interval
            yield self.run_date


class DateTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'date'}

    def __init__(self, command_args: str, date: datetime):
        super().__init__(command_args, date.isoformat())

    def get_next_run_date_iter(self) -> Iterator[datetime]:
        pass

    def reset_iter(self) -> NoReturn:
        pass


class TaskFactory:
    _trigger_type_mapping = {
        'cron': CronTask,
        'interval': IntervalTask,
        'date': DateTask
    }

    @staticmethod
    def create(command_args: str, trigger_type: str, trigger_args):
        TaskClass = TaskFactory._trigger_type_mapping.get(trigger_type)
        if not TaskClass:
            raise ValueError(f"No such trigger type '{trigger_type}'")
        return TaskClass(command_args, **trigger_args)