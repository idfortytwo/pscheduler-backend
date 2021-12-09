from __future__ import annotations

import abc
import datetime

from enum import Enum, auto

from sqlalchemy import Column, Text, Integer, DateTime, ForeignKey, Index
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


class ExecutionLog(Base):
    __tablename__ = 'execution_log'

    execution_log_id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey('task.task_id'), nullable=False)
    status = Column(Text, nullable=False)
    start_date = Column(DateTime, default=datetime.datetime.utcnow)
    finish_date = Column(DateTime)
    return_code = Column(Integer)

    def __init__(self, task_id: int, start_date: datetime.datetime = None):
        self.task_id = task_id
        if start_date:
            self.start_date = start_date
        self.set_state(ExecutionState.AWAITING)

    def set_state(self, state: ExecutionState):
        self.status = state.name.lower()

    def to_dict(self):
        return {
            k: v
            for k, v
            in self.__dict__.items()
            if k in self.__table__.columns
        }


class ExecutionState(Enum):
    AWAITING = auto()
    STARTED = auto()
    FINISHED = auto()
    FAILED = auto()
    MISSED = auto()


class ExecutionOutputLog(Base):
    __tablename__ = 'execution_output_log'

    execution_output_log_id = Column(Integer, primary_key=True, autoincrement=True)
    execution_log_id = Column(Integer, ForeignKey('execution_log.execution_log_id'), nullable=False)
    message = Column(Text, nullable=False)
    time = Column(DateTime, nullable=False)

    __table_args__ = (
        Index('ids_index', 'execution_output_log_id', 'execution_log_id', unique=True),
    )

    def __init__(self, message: str, time: datetime.datetime, task_run_id: int):
        self.execution_log_id = task_run_id
        self.message = message
        self.time = time

    def to_dict(self):
        return {
            k: v
            for k, v
            in self.__dict__.items()
            if k in self.__table__.columns
        }