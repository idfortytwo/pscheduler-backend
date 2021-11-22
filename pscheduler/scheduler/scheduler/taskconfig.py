import ast

from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta
from typing import Type, List, Iterator

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from db.connection import Session
from db.models import TaskConfigModel


class TaskConfig(TaskConfigModel, metaclass=ABCMeta):
    @property
    @abstractmethod
    def _trigger_type(self) -> str:
        pass

    @abstractmethod
    def __init__(self, command_args: str, schedule_params: any, /, *,
                 starting_date: datetime = None):
        self.command_args = command_args
        self.trigger_args = schedule_params
        self.starting_date = starting_date
        self.trigger_type = self._trigger_type

    @abstractmethod
    def get_next_run_date_it(self) -> Iterator[datetime]:
        pass

    def __repr__(self):
        return f'{self.__class__.__name__}(\'{self.command_args}\', {self.trigger_args})'


class CronTaskConfig(TaskConfig):
    _trigger_type = 'cron'

    def __init__(self, command_args: str, schedule_params: any,
                 /, *, starting_date: datetime = None):
        super().__init__(command_args, schedule_params, starting_date=starting_date)

    def get_next_run_date_it(self) -> Iterator[datetime]:
        pass


class IntervalTaskConfig(TaskConfig):
    _trigger_type = 'interval'

    def __init__(self, command_args: str,
                 /, *, starting_date=datetime.now(),
                 days=0, seconds=0, minutes=0, hours=0, weeks=0):
        kwargs = {
            'days': days,
            'seconds': seconds,
            'minutes': minutes,
            'hours': hours,
            'weeks': weeks
        }
        trigger_args = {k: v for k, v in kwargs.items() if v}
        super().__init__(command_args, str(trigger_args), starting_date=starting_date)

    @property
    def interval(self):
        args = ast.literal_eval(self.trigger_args)
        return timedelta(**args)

    def get_next_run_date_it(self) -> Iterator[datetime]:
        while True:
            if not hasattr(self, 'run_date'):
                self.run_date = datetime.utcnow() - self.interval
            self.run_date += self.interval
            yield self.run_date


class DateTaskConfig(TaskConfig):
    _trigger_type = 'date'

    def __init__(self, command_args: str, date: datetime):
        super().__init__(command_args, date.isoformat())

    def get_next_run_date_it(self) -> Iterator[datetime]:
        pass


class TaskConfigs:
    _config_types: [Type[TaskConfig]] = [CronTaskConfig, IntervalTaskConfig, DateTaskConfig]

    @staticmethod
    async def _get_query_for_class(session: AsyncSession, cls: Type[TaskConfig]) -> [Type[TaskConfig]]:
        q = select(cls).filter(cls.trigger_type == cls._trigger_type)
        return (await session.execute(q)).fetchall()

    @classmethod
    async def fetch(cls, session=Session()) -> List[TaskConfig]:
        return [
            config_row[0]
            for config_type in cls._config_types
            for config_row in await cls._get_query_for_class(session, config_type)
        ]