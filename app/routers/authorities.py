import ujson

from fastapi import APIRouter, Depends

from models.authorities import AuthorityRecord
from clients.redis_clients.redis_connector import RedisClientTuple, RedisAsyncConnector
from utils.marc_utils import convert_nlp_id_auth_to_sierra_format


router = APIRouter(tags=["authorities"])


@router.get('/api/authorities/{auth_ids}', response_model=list[AuthorityRecord])
async def get_single_or_multiple_authorities(
        auth_ids: str,
        redis_client: RedisClientTuple = Depends(RedisAsyncConnector.get_redis_client)):
    """
    Get single authority by its id or multiple authorities using comma-separated list of ids.
    You will get them wrapped in a list in either case.
    """
    auth_ids_as_list = [auth_id for auth_id in auth_ids.split(',')]
    joined_dict = {}

    resp_int_ids = await redis_client.auth_int.mget(*auth_ids_as_list)
    for auth, auth_ids in zip(auth_ids, resp_int_ids):
        if auth_ids:
            joined_dict.setdefault(auth, {}).setdefault('ids_from_internal', {}).update(ujson.loads(auth_ids))
        else:
            joined_dict.setdefault(auth, {}).setdefault('ids_from_internal', None)

    # we have to convert authorities ids to the sierra format to get the external ids
    auth_ids_converted = {
        auth_id: convert_nlp_id_auth_to_sierra_format(auth_id) for auth_id in auth_ids_as_list}

    resp_ext_ids = await redis_client.auth_ext.mget(*list(auth_ids_converted.values()))
    for auth, auth_ids in zip(list(auth_ids_converted.keys()), resp_ext_ids):
        if auth_ids:
            joined_dict.setdefault(auth, {}).setdefault('ids_from_external', {}).update(ujson.loads(auth_ids))
        else:
            joined_dict.setdefault(auth, {}).setdefault('ids_from_external', None)

    return [v for v in joined_dict.values()]
