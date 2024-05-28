import os

from aiohttp import ClientSession
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi.responses import UJSONResponse
from fastapi.security import APIKeyQuery


from app.clients.http_clients.http_connector import HttpAsyncConnector
from app.clients.redis_clients.redis_connector import RedisAsyncConnector, RedisClientTuple
from app.updaters.authorities_updater import AuthorityUpdater
from app.updaters.authorities_updater_client import AuthorityUpdaterClient
from app.updaters.authorities_updater_background_tasks import do_authority_update

API_KEY_UPDATER = os.getenv('API_KEY_UPDATER')


router = APIRouter(tags=["updaters"])

query_scheme = APIKeyQuery(name="api_key")


# TODO switch include_in_schema to False after tests!
@router.get('/updater/authorities/{database_type}',
            response_class=UJSONResponse,
            include_in_schema=True)
async def get(self,
              database_type: str,
              background_tasks: BackgroundTasks,
              api_key: str = Depends(query_scheme),
              http_client: ClientSession = Depends(HttpAsyncConnector.get_http_client),
              redis_client: RedisClientTuple = Depends(RedisAsyncConnector.get_redis_client),
              authority_updater: AuthorityUpdater = Depends(AuthorityUpdaterClient.get_authority_updater())):
    if authority_updater.update_in_progress:
        return {'update_in_progress': authority_updater.update_in_progress,
                'last_update': str(authority_updater.last_authorities_update),
                'message': 'Update in progress... Try again later.'}
    else:
        background_tasks.add_task(do_authority_update,
                                  authority_updater,
                                  http_client,
                                  redis_client.auth_int)
        return {'update_in_progress': authority_updater.update_in_progress,
                'last_update': str(authority_updater.last_authorities_update),
                'message': 'Started updating authorities.'}


# TODO switch include_in_schema to False after tests!
@router.get('/updater/authorities/{database_type}/status',
            response_class=UJSONResponse,
            include_in_schema=True)
async def get(self,
              database_type: str,
              authority_updater: AuthorityUpdater = Depends(AuthorityUpdaterClient.get_authority_updater())):
    return {'update_in_progress': authority_updater.update_in_progress,
            'last_update': str(authority_updater.last_authorities_update)}
