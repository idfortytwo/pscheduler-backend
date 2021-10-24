import abc

from sqlalchemy import Column, Text, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, DeclarativeMeta


class DeclarativeABCMeta(DeclarativeMeta, abc.ABCMeta):
    pass


Base = declarative_base(metaclass=DeclarativeABCMeta)


class EventModel(Base):
    __tablename__ = 'events'

    event_id = Column(Integer, primary_key=True, autoincrement=True)
    event_type_id = Column(Integer, ForeignKey('event_types.event_type_id'), nullable=False)
    command_args = Column(Text, nullable=False)
    schedule_data = Column(Text, nullable=False)

    def __repr__(self):
        return f'EventModel({self.event_id}, {self.event_type_id}, {self.command_args}, {self.schedule_data})'


class EventTypeModel(Base):
    __tablename__ = 'event_types'

    event_type_id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(Text, nullable=False, unique=True)

    def __init__(self, event_type: str, event_type_id: int = None):
        self.event_type = event_type
        if self.event_type_id:
            self.event_type_id = event_type_id

    def __repr__(self):
        return f'EventTypeModel({self.event_type_id}, {self.event_type})'