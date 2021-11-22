import abc

from sqlalchemy import Column, Text, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, DeclarativeMeta


class DeclarativeABCMeta(DeclarativeMeta, abc.ABCMeta):
    pass


Base = declarative_base(metaclass=DeclarativeABCMeta)


class TaskConfigModel(Base):
    __tablename__ = 'events'

    task_config_id = Column(Integer, primary_key=True, autoincrement=True)
    command_args = Column(Text, nullable=False)
    trigger_type = Column(Text, nullable=False)
    trigger_args = Column(Text, nullable=False)
    starting_date = Column(DateTime)
    last_run = Column(DateTime)

    def __repr__(self):
        return f'EventModel({self.task_config_id}, {self.trigger_type}, {self.command_args}, {self.trigger_args})'