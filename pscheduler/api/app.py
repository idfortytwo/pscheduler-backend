from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from uvicorn import Server, Config

from api.routers import router


app = FastAPI()
app.include_router(router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

config = Config(app=app, host='127.0.0.1', port=8000, loop='asyncio')
server = Server(config)
