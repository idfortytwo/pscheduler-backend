import json

from datetime import datetime, timedelta
from abc import abstractmethod, ABC
from typing import Iterator, Dict
from croniter import croniter

from db.models import TaskModel


class Task(TaskModel, ABC):
    def __init__(self, title: str, command: str, trigger_args: any, descr: str):
        self.title = title
        self.command = command
        self.trigger_args = trigger_args
        self.descr = descr

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

    def __init__(self, title: str, command: str, cron_string: str, descr: str = ''):
        super().__init__(title, command, trigger_args=cron_string, descr=descr)

    @property
    def run_date_iter(self) -> Iterator[datetime]:
        return croniter(self.trigger_args, ret_type=datetime)


class IntervalTask(Task):
    __mapper_args__ = {'polymorphic_identity': 'interval'}

    def __init__(self, title: str, command: str, *,
                 days=0, seconds=0, minutes=0, hours=0, weeks=0, descr: str = '', trigger_args=None):
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

        super().__init__(title, command, trigger_args=json.dumps(trigger_args), descr=descr)

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

    def __init__(self, title: str, command: str, date: datetime, descr: str = ''):
        super().__init__(title, command, trigger_args=date, descr=descr)

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
    def create(title: str, command: str, trigger_type: str, trigger_args_str: str, descr: str = ''):
        TaskClass = TaskFactory._get_class(trigger_type)
        return TaskClass(title, command, trigger_args=trigger_args_str, descr=descr)

    @staticmethod
    def create_from_kwargs(title: str, command: str, trigger_type: str, trigger_kwargs: Dict, descr: str = ''):
        TaskClass = TaskFactory._get_class(trigger_type)
        return TaskClass(title, command, **trigger_kwargs, descr=descr)