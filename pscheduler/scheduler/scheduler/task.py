import ast

from datetime import datetime, timedelta
from abc import abstractmethod, ABC
from typing import Iterator, NoReturn

from db.models import TaskModel


class Task(TaskModel, ABC):
    @abstractmethod
    def __init__(self, command: str, schedule_params: any):
        self.command = command
        self.trigger_args = schedule_params

    @abstractmethod
    def get_next_run_date_iter(self) -> Iterator[datetime]:
        pass

    @abstractmethod
    def reset_iter(self) -> NoReturn:
        pass

    def to_dict(self):
        return {
            k: v
            for k, v in self.__dict__.items()
            if k in self.__table__.columns
        }

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.command}\', {self.trigger_args})'


class CronTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'cron'}

    def __init__(self, command: str, schedule_params: any):
        super().__init__(command, schedule_params)

    def get_next_run_date_iter(self) -> Iterator[datetime]:
        pass

    def reset_iter(self) -> NoReturn:
        pass


class IntervalTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'interval'}

    def __init__(self, command: str, /, *,
                 days=0, seconds=0, minutes=0, hours=0, weeks=0):
        kwargs = {
            'days': days,
            'seconds': seconds,
            'minutes': minutes,
            'hours': hours,
            'weeks': weeks
        }
        trigger_args = {k: v for k, v in kwargs.items() if v}
        super().__init__(command, str(trigger_args))

    def to_dict(self):
        _dict = super().to_dict()
        _dict['trigger_args'] = ast.literal_eval(_dict['trigger_args'])
        return _dict

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

    def __init__(self, command: str, date: datetime):
        super().__init__(command, date.isoformat())

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
    def create(command: str, trigger_type: str, trigger_args):
        TaskClass = TaskFactory._trigger_type_mapping.get(trigger_type)
        if not TaskClass:
            raise ValueError(f"No such trigger type '{trigger_type}'")
        return TaskClass(command, **trigger_args)