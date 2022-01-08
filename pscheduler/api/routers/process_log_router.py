from fastapi import Depends

from api.routers._shared import router
from db.dal import DAL, get_dal


@router.get('/process_log', status_code=200)
async def get_process_logs(db: DAL = Depends(get_dal)):
    process_logs = await db.get_process_logs()

    return {'process_logs': [
        log.to_dict()
        for log
        in process_logs
    ]}