from typing import List

from sqlalchemy import select

from api.routers._shared import router
from db.connection import Session
from db.models import ExecutionLog


@router.get('/execution_log', status_code=200)
async def get_execution_logs():
    async with Session() as session:
        from typing import Union
        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import Session as NormalSession
        session: Union[NormalSession, AsyncSession]

        execution_logs: List[ExecutionLog] = await session.scalars(select(ExecutionLog))

        return {'execution_logs': [
            log.to_dict()
            for log
            in execution_logs
        ]}