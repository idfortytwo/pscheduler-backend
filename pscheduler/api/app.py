from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from uvicorn import Server, Config
from dotenv import dotenv_values

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

config = dotenv_values('.env')
host = config['APP_HOST']
port = int(config['APP_PORT'])

config = Config(app=app, host=host, port=port, loop='asyncio')
server = Server(config)
