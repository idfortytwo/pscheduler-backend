from typing import Optional

from fastapi import Depends

from api.routers._shared import router
from db.dal import DAL, get_dal


@router.get('/execution/output/{execution_log_id}', status_code=200)
async def get_execution_output_logs(execution_log_id: int, last_execution_output_log_id: Optional[int] = None,
                                    db: DAL = Depends(get_dal)):
    execution_output_logs = await db.get_execution_output_logs(execution_log_id, last_execution_output_log_id)
    if execution_output_logs:
        last_execution_output_log_id = execution_output_logs[-1].execution_output_log_id

    status = None
    return_code = None
    if not execution_output_logs:
        execution_log = await db.get_execution_log(execution_log_id)
        status = execution_log.status
        return_code = execution_log.return_code

    return {
        'execution_output_logs': [
            log.to_dict()
            for log
            in execution_output_logs
        ],
        'last_execution_output_log_id': last_execution_output_log_id,
        'status': status,
        'return_code': return_code
    }
