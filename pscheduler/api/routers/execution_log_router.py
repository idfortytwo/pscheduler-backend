from fastapi import Depends

from api.routers._shared import router
from db.dal import DAL, get_dal


@router.get('/execution_log', status_code=200)
async def get_execution_logs(db: DAL = Depends(get_dal)):
    execution_logs = await db.get_execution_logs()

    return {'execution_logs': [
        log.to_dict()
        for log
        in execution_logs
    ]}