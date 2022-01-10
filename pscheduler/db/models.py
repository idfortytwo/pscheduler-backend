from __future__ import annotations

from abc import ABCMeta, abstractmethod
import datetime

from enum import Enum, auto

from sqlalchemy import Column, Text, Integer, DateTime, ForeignKey, Index
from sqlalchemy.orm import declarative_base, DeclarativeMeta


class DeclarativeABCMeta(DeclarativeMeta, ABCMeta):
    pass


Base = declarative_base(metaclass=DeclarativeABCMeta)


class TaskModel(Base):
    __tablename__ = 'task'

    task_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text, nullable=False)
    descr = Column(Text)
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


class ProcessLog(Base):
    __tablename__ = 'process_log'

    process_log_id = Column(Integer, primary_key=True, autoincrement=True)
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
        self.state = state

    @property
    def state(self) -> ExecutionState:
        return getattr(ExecutionState, self.status.upper())

    @state.setter
    def state(self, state: ExecutionState):
        self._state = state

    def to_dict(self):
        return {
            k: v
            for k, v
            in self.__dict__.items()
            if k in self.__table__.columns
        }

    def __repr__(self):
        return f"ProcessLog({self.task_id}, '{self.status}', {self.start_date}, {self.finish_date})"


class ExecutionState(Enum):
    AWAITING = auto()
    STARTED = auto()
    FINISHED = auto()
    FAILED = auto()
    MISSED = auto()


class OutputLog(Base, metaclass=ABCMeta):
    __tablename__ = 'output_log'
    __table_args__ = (
        Index('ids_index', 'output_log_id', 'process_log_id', unique=True),
    )

    output_log_id = Column(Integer, primary_key=True, autoincrement=True)
    process_log_id = Column(Integer, ForeignKey('process_log.process_log_id'), nullable=False)
    message = Column(Text, nullable=False)
    time = Column(DateTime, nullable=False)
    is_error = Column(Integer, nullable=False)

    __mapper_args__ = {
        'polymorphic_on': is_error
    }

    def to_dict(self):
        dct = {
            k: v
            for k, v
            in self.__dict__.items()
            if k in self.__table__.columns and k != 'error'
        }
        dct['error'] = bool(self.is_error)
        return dct

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.message.rstrip()}', {self.output_log_id}, {self.process_log_id})"

    @abstractmethod
    def __init__(self, message: str, time: datetime.datetime, process_log_id: int, is_error: int):
        self.process_log_id = process_log_id
        self.message = message
        self.time = time
        self.is_error = is_error


class ConsoleLog(OutputLog):
    __mapper_args__ = {
        'polymorphic_identity': 0
    }

    def __init__(self, message: str, time: datetime.datetime, process_log_id: int):
        super().__init__(message, time, process_log_id, is_error=0)


class StderrLog(OutputLog):
    __mapper_args__ = {
        'polymorphic_identity': 1
    }

    def __init__(self, message: str, time: datetime.datetime, process_log_id: int):
        super().__init__(message, time, process_log_id, is_error=1)