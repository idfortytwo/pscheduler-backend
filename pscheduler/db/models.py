from __future__ import annotations

import abc
import datetime

from enum import Enum, auto

from sqlalchemy import Column, Text, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, DeclarativeMeta


class DeclarativeABCMeta(DeclarativeMeta, abc.ABCMeta):
    pass


Base = declarative_base(metaclass=DeclarativeABCMeta)


class TaskModel(Base):
    __tablename__ = 'task'

    task_id = Column(Integer, primary_key=True, autoincrement=True)
    command = Column(Text, nullable=False)
    trigger_type = Column(Text, nullable=False)
    trigger_args = Column(Text, nullable=False)
    starting_date = Column(DateTime)
    last_run = Column(DateTime)

    __mapper_args__ = {
        'polymorphic_on': trigger_type,
        'polymorphic_identity': 'task'
    }

    def __repr__(self):
        return f'TaskModel({self.task_id}, {self.trigger_type}, {self.command}, {self.trigger_args})'


class TaskRunLog(Base):
    __tablename__ = 'task_run_log'

    task_run_id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey('task.task_id'), nullable=False)
    status = Column(Text, nullable=False)
    start_date = Column(DateTime, default=datetime.datetime.utcnow)
    finish_date = Column(DateTime)
    return_code = Column(Integer)

    def __init__(self, task_id: int):
        self.task_id = task_id
        self.set_state(ExecutionState.AWAITING)

    def set_state(self, state: ExecutionState):
        self.status = state.name.lower()


class ExecutionState(Enum):
    AWAITING = auto()
    STARTED = auto()
    FINISHED = auto()
    FAILED = auto()


class TaskOutputLog(Base):
    __tablename__ = 'task_output_log'

    task_output_log_id = Column(Integer, primary_key=True, autoincrement=True)
    task_run_id = Column(Integer, ForeignKey('task_run_log.task_run_id'), nullable=False)
    value = Column(Text, nullable=False)
    date = Column(DateTime, nullable=False)

    def __init__(self, value: str, date: datetime.datetime, task_run_id: int):
        self.value = value
        self.date = date
        self.task_run_id = task_run_id