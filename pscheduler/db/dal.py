import json
from typing import List

from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import TaskInputModel
from db.connection import Session
from db.models import ProcessLog, StdoutLog
from scheduler.executor import ExecutionManager
from scheduler.task import Task, TaskFactory


class DAL:
    def __init__(self, db_session: AsyncSession, execution_manager: ExecutionManager):
        self.session = db_session
        self.execution_manager = execution_manager

    async def get_tasks(self) -> List[Task]:
        rs = await self.session.execute(
            select(Task)
        )
        return rs.scalars().all()

    async def get_task(self, task_id: int):
        rs = await self.session.execute(
            select(Task).
            filter(Task.task_id == task_id)
        )
        return rs.scalar()

    async def add_task(self, task: TaskInputModel):
        new_task = TaskFactory.create(task.title, task.command, task.trigger_type, task.trigger_args, descr=task.descr)
        self.session.add(new_task)

        await self.session.commit()
        await self.execution_manager.sync()
        return new_task

    async def delete_task(self, task_id: int):
        await self.session.execute(
            delete(Task).
            filter(Task.task_id == task_id)
        )
        await self.session.commit()
        await self.execution_manager.sync()

    async def update_task(self, task_id: int, task: TaskInputModel):
        await self.session.execute(
            update(Task).
            filter(Task.task_id == task_id).
            values(
                command=task.command,
                title=task.title,
                descr=task.descr,
                trigger_args=json.dumps(task.trigger_args).strip('"'),
                trigger_type=task.trigger_type
            )
        )
        await self.session.commit()
        await self.execution_manager.sync()

    async def get_execution_logs(self) -> List[ProcessLog]:
        rs = await self.session.execute(
            select(ProcessLog).
            order_by(ProcessLog.process_log_id)
        )
        return rs.scalars().all()

    async def get_execution_log(self, exec_log_id: int) -> ProcessLog:
        rs = await self.session.execute(
            select(ProcessLog).
            filter(ProcessLog.process_log_id == exec_log_id)
        )
        return rs.scalar()

    async def get_execution_output_logs(self, exec_log_id: int,
                                        last_exec_output_log_id: int = None) -> List[StdoutLog]:
        q = select(StdoutLog).filter(StdoutLog.execution_log_id == exec_log_id)
        if last_exec_output_log_id:
            q = q.filter(StdoutLog.execution_output_log_id > last_exec_output_log_id)

        rs = await self.session.execute(q)
        return rs.scalars().all()


async def get_dal():
    async with Session(expire_on_commit=False) as session:
        async with session.begin():
            yield DAL(session, ExecutionManager())