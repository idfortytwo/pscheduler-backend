from sqlalchemy import Column, Text, Integer, DateTime
from sqlalchemy.orm import declarative_base, DeclarativeMeta


Base = declarative_base(metaclass=DeclarativeMeta)


class TaskConfigModel(Base):
    __tablename__ = 'task'

    task_config_id = Column(Integer, primary_key=True, autoincrement=True)
    command_args = Column(Text, nullable=False)
    trigger_type = Column(Text, nullable=False)
    trigger_args = Column(Text, nullable=False)
    starting_date = Column(DateTime)
    last_run = Column(DateTime)

    __mapper_args__ = {
        'polymorphic_on': trigger_type
    }

    def __repr__(self):
        return f'TaskConfigModel({self.task_config_id}, {self.trigger_type}, {self.command_args}, {self.trigger_args})'