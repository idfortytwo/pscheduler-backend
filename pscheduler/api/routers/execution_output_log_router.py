from typing import List, Optional

from sqlalchemy import select

from api.routers._shared import router
from db.connection import Session
from db.models import ExecutionOutputLog, ExecutionLog


@router.get('/execution/output/{execution_log_id}', status_code=200)
async def get_execution_output_log(execution_log_id: int, last_execution_output_log_id: Optional[int] = None):
    async with Session() as session:
        select_stmt = select(ExecutionOutputLog)
        select_stmt = select_stmt.filter(ExecutionOutputLog.execution_log_id == execution_log_id)
        if last_execution_output_log_id:
            select_stmt = select_stmt.filter(ExecutionOutputLog.execution_output_log_id > last_execution_output_log_id)

        execution_output_logs: List[ExecutionOutputLog] = list(await session.scalars(select_stmt))
        if execution_output_logs:
            last_execution_output_log_id = execution_output_logs[-1].execution_output_log_id

        status = None
        if not execution_output_logs:
            exec_log_stmt = select(ExecutionLog).filter(ExecutionLog.execution_log_id == execution_log_id)
            execution_log: ExecutionLog = await session.scalar(exec_log_stmt)
            status = execution_log.status

        return {
            'execution_output_logs': [
                log.to_dict()
                for log
                in execution_output_logs
            ],
            'last_execution_output_log_id': last_execution_output_log_id,
            'status': status
        }