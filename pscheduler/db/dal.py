from typing import List

from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import TaskInputModel
from db.connection import Session
from db.models import ExecutionLog, ExecutionOutputLog
from scheduler.task import Task, TaskFactory


class DAL:
    def __init__(self, db_session: AsyncSession):
        self.session = db_session

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
        new_task = TaskFactory.create(task.command, task.trigger_type, task.trigger_args)
        self.session.add(new_task)

        await self.session.commit()
        await self.session.refresh(new_task)
        return new_task

    async def delete_task(self, task_id: int):
        await self.session.execute(
            delete(Task).
            filter(Task.task_id == task_id)
        )
        await self.session.commit()

    async def update_task(self, task_id: int, task: TaskInputModel):
        await self.session.execute(
            update(Task).
            filter(Task.task_id == task_id).
            values(
                trigger_args=str(task.trigger_args),
                command=task.command,
                trigger_type=task.trigger_type
            )
        )
        await self.session.commit()

    async def get_execution_logs(self) -> List[ExecutionLog]:
        rs = await self.session.execute(
            select(ExecutionLog)
        )
        return rs.scalars().all()

    async def get_execution_log(self, exec_log_id: int) -> ExecutionLog:
        rs = await self.session.execute(
            select(ExecutionLog).
            filter(ExecutionLog.execution_log_id == exec_log_id)
        )
        return rs.scalar()

    async def get_execution_output_logs(self, exec_log_id: int,
                                        last_exec_output_log_id: int = None) -> List[ExecutionOutputLog]:
        q = select(ExecutionOutputLog).filter(ExecutionOutputLog.execution_log_id == exec_log_id)
        if last_exec_output_log_id:
            q = q.filter(ExecutionOutputLog.execution_output_log_id > last_exec_output_log_id)

        rs = await self.session.execute(q)
        return rs.scalars().all()


async def get_dal():
    async with Session() as session:
        async with session.begin():
            yield DAL(session)