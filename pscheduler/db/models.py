import abc

from sqlalchemy import Column, Text, Integer, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, DeclarativeMeta


class DeclarativeABCMeta(DeclarativeMeta, abc.ABCMeta):
    pass


Base = declarative_base(metaclass=DeclarativeABCMeta)


class EventModel(Base):
    __tablename__ = 'events'

    event_id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(Text, nullable=False)
    command_args = Column(Text, nullable=False)
    schedule_params = Column(Text, nullable=False)
    starting_date = Column(DateTime)
    last_run = Column(DateTime)

    def __repr__(self):
        return f'EventModel({self.event_id}, {self.event_type}, {self.command_args}, {self.schedule_params})'