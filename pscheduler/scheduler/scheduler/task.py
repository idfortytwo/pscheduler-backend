import json

from datetime import datetime, timedelta
from abc import abstractmethod, ABC
from typing import Iterator, Dict
from croniter import croniter

from db.models import TaskModel


class Task(TaskModel, ABC):
    def __init__(self, command: str, trigger_args: any):
        self.command = command
        self.trigger_args = trigger_args

    @property
    @abstractmethod
    def run_date_iter(self) -> Iterator[datetime]:
        pass

    def to_dict(self):
        return {
            k: v
            for k, v in self.__dict__.items()
            if k in self.__table__.columns
        }

    def __hash__(self):
        return hash((self.command, self.trigger_args, self.trigger_type))

    def __eq__(self, other):
        if isinstance(other, Task):
            return hash(self) == hash(other)

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.command}\', {self.trigger_args})'


class CronTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'cron'}

    def __init__(self, command: str, cron: str):
        super().__init__(command, cron)

    @property
    def run_date_iter(self) -> Iterator[datetime]:
        return croniter(self.trigger_args, ret_type=datetime)


class IntervalTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'interval'}

    def __init__(self, command: str, trigger_args=None, *,
                 days=0, seconds=0, minutes=0, hours=0, weeks=0):
        if trigger_args is None:
            trigger_args = {
                'days': days,
                'seconds': seconds,
                'minutes': minutes,
                'hours': hours,
                'weeks': weeks
            }

        trigger_args = {
            k: v
            for k, v in trigger_args.items()
            if self.validate(v)
        }

        if timedelta(**trigger_args) == timedelta():
            raise ValueError('interval should be greater than 0')

        super().__init__(command, json.dumps(trigger_args))

    @staticmethod
    def validate(value: int):
        if value is None:
            return False
        else:
            return value > 0

    def to_dict(self):
        _dict = super().to_dict()
        _dict['trigger_args'] = json.loads(_dict['trigger_args'])
        return _dict

    @property
    def interval(self):
        args = json.loads(self.trigger_args)
        return timedelta(**args)

    @property
    def run_date_iter(self) -> Iterator[datetime]:
        self.run_date = datetime.utcnow()

        while True:
            self.run_date += self.interval
            yield self.run_date


class DateTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'date'}

    def __init__(self, command: str, date: datetime):
        super().__init__(command, date)

    @property
    def run_date_iter(self) -> Iterator[datetime]:
        yield self.trigger_args
        while True:
            yield None


class TaskFactory:
    _trigger_type_mapping = {
        'cron': CronTask,
        'interval': IntervalTask,
        'date': DateTask
    }

    @staticmethod
    def _get_class(trigger_type: str):
        TaskClass = TaskFactory._trigger_type_mapping.get(trigger_type)
        if not TaskClass:
            raise ValueError(f"No such trigger type '{trigger_type}'")
        return TaskClass

    @staticmethod
    def create(command: str, trigger_type: str, trigger_args_str: str):
        TaskClass = TaskFactory._get_class(trigger_type)
        return TaskClass(command, trigger_args_str)

    @staticmethod
    def create_from_kwargs(command: str, trigger_type: str, trigger_kwargs: Dict):
        TaskClass = TaskFactory._get_class(trigger_type)
        return TaskClass(command, **trigger_kwargs)