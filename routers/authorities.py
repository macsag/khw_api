imprt ujson

from fastapi import APIRouter, Depends
from aioredis import Redis

router = APIRouter(tags=["authorities"])


@router.get("/api/libraries/{library_id}", response_model=LibraryOut)
async def get_single_library(
        library_id: str,
        mongo_client: AsyncIOMotorClient = Depends(MongoDBAsyncConnector.get_mongo_client)
    ):
    async with await mongo_client.start_session() as mongo_session:
        result = await do_mongo_find(
            mongo_client,
            mongo_session,
            'gargamedon',
            'libraries',
            single_object_id=library_id)
        return LibraryOut.from_mongo(result)


# return authorities (single or more) with internal and external ids in json
@router.get('/api/authorities/{authority_ids}', response model=AuthorityList)
    async def get_single_or_multiple_authorities(
            authority_ids: str,
            redis_client_internal_ids: Redis = Depends(RedisAsyncConnector.get_redis_client)
    ):
        authority_ids_as_list = [auth_id for auth_id in authority_ids.split(',')]
        response_internal_ids = await redis_client_internal_ids.mget(*authority_ids_as_list)

        joined_dict = {}
        for auth, auth_ids in zip(authority_ids, response_internal_ids):
            if auth_ids:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_internal', {}).update(ujson.loads(auth_ids))
            else:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_internal', None)

        # transform authority ids to sierra format to get external ids
        authority_ids_transformed_as_dict = {auth_id: convert_nlp_id_auth_to_sierra_format(auth_id) for auth_id in authority_ids_as_list}
        resp_2 = await conn_auth_ext.mget(*list(authority_ids_transformed_to_sierra_format.values()))

        for auth, auth_ids in zip(list(authority_ids_transformed_to_sierra_format.keys()), resp_2):
            if auth_ids:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_external', {}).update(json.loads(auth_ids))
            else:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_external', None)

        return JSONResponse(joined_dict)