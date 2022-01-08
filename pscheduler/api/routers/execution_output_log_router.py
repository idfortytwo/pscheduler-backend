from typing import Optional

from fastapi import Depends

from api.routers._shared import router
from db.dal import DAL, get_dal


@router.get('/execution/output/{process_log_id}', status_code=200)
async def get_output_logs(process_log_id: int, last_output_log_id: Optional[int] = None,
                          db: DAL = Depends(get_dal)):
    output_logs = await db.get_output_logs(process_log_id, last_output_log_id)
    if output_logs:
        last_output_log_id = output_logs[-1].output_log_id

    process_log = await db.get_process_log(process_log_id)
    status = process_log.status
    return_code = process_log.return_code

    return {
        'output_logs': [
            log.to_dict()
            for log
            in output_logs
        ],
        'last_output_log_id': last_output_log_id,
        'status': status,
        'return_code': return_code
    }
