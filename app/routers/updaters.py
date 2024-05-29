import os

from aiohttp import ClientSession
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi.responses import UJSONResponse
from fastapi.security import APIKeyQuery


from clients.http_clients.http_connector import HttpAsyncConnector
from clients.redis_clients.redis_connector import RedisAsyncConnector, RedisClientTuple
from models.updaters import UpdaterMessage
from authorities_updater import AuthorityUpdater
from authorities_updater_client import AuthorityUpdaterClient
from authorities_updater_background_tasks import do_authority_update


load_dotenv()

API_KEY_UPDATER = os.getenv('API_KEY_UPDATER')


router = APIRouter(tags=["updaters"])
query_scheme = APIKeyQuery(name="api_key")


# TODO switch include_in_schema to False after tests!
@router.get('/updater/authorities/{database_type}',
            response_class=UJSONResponse,
            response_model=UpdaterMessage,
            include_in_schema=True)
async def get(self,
              database_type: str,
              background_tasks: BackgroundTasks,
              api_key: str = Depends(query_scheme),
              http_client: ClientSession = Depends(HttpAsyncConnector.get_http_client),
              redis_client: RedisClientTuple = Depends(RedisAsyncConnector.get_redis_client),
              authority_updater: AuthorityUpdater = Depends(AuthorityUpdaterClient.get_authority_updater())):
    if authority_updater.update_in_progress:
        return {'is_update_in_progress': authority_updater.update_in_progress,
                'last_update_time': str(authority_updater.last_authorities_update),
                'message': 'Update in progress... Try again later.'}
    else:
        background_tasks.add_task(do_authority_update,
                                  authority_updater,
                                  http_client,
                                  redis_client.auth_int)
        return {'update_in_progress': authority_updater.update_in_progress,
                'last_update_time': str(authority_updater.last_authorities_update),
                'message': 'Scheduled authorities update.'}


# TODO switch include_in_schema to False after tests!
@router.get('/updater/authorities/{database_type}/status',
            response_class=UJSONResponse,
            response_model=UpdaterMessage,
            include_in_schema=True)
async def get(self,
              database_type: str,
              authority_updater: AuthorityUpdater = Depends(AuthorityUpdaterClient.get_authority_updater())):
    is_update_in_progress = authority_updater.update_in_progress
    message = 'No authorities update job running. You can always schedule the new one.'
    if is_update_in_progress:
        message = 'Update in progress...'

    return {'is_update_in_progress': is_update_in_progress,
            'last_update_time': str(authority_updater.last_authorities_update),
            'message': message}
