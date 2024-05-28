import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
import uvicorn

from applog.utils import read_logging_config, setup_logging
from clients.redis_clients.redis_connector import RedisAsyncConnector
from clients.http_clients.http_connector import HttpAsyncConnector
from routers.authorities import router as authorities_router
from routers.bibs import router as bibs_router
from routers.polona_lod import router as polona_lod_router
from routers.updaters import router as updaters_router

load_dotenv()

IS_LOCAL = os.getenv('IS_LOCAL')

LOC_HOST = os.getenv('LOC_HOST')
LOC_PORT = int(os.getenv('LOC_PORT'))

PROD_HOST = os.getenv('PROD_HOST')
PROD_PORT = int(os.getenv('PROD_PORT'))


logconfig_dict = read_logging_config('applog/logging.yml')
setup_logging(logconfig_dict)

templates = Jinja2Templates(directory='templates')


async def on_start_up() -> None:
    await RedisAsyncConnector.create_redis_client()
    await HttpAsyncConnector.create_http_client()


async def on_shutdown() -> None:
    await RedisAsyncConnector.close_redis_client()
    await HttpAsyncConnector.close_http_client()

app = FastAPI(on_startup=[on_start_up],
              on_shutdown=[on_shutdown])

app.include_router(authorities_router.router)
app.include_router(bibs_router)
app.include_router(polona_lod_router)
app.include_router(updaters_router)


# homepage
@app.get('/')
async def homepage(request):
    return 'ok'


if __name__ == '__main__':

    if IS_LOCAL:
        uvicorn.run('app:app', host=LOC_HOST, port=LOC_PORT)
    else:
        uvicorn.run('app:app', host=PROD_HOST, port=PROD_PORT, workers=3)
