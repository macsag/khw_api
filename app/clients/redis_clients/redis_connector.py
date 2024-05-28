import os
from collections import namedtuple

import aioredis
from dotenv import load_dotenv

load_dotenv('....')


REDIS_ADDRESS = f'{os.getenv("REDIS_HOST")}:{os.getenv("REDIS_PORT")}'
REDIS_AUTH_INT_DB = os.getenv('REDIS_AUTH_INT_DB')
REDIS_AUTH_EXT_DB = os.getenv('REDIS_AUTH_EXT_DB')


RedisClientTuple = namedtuple('RedisClientTuple', 'auth_int auth_ext')


class RedisAsyncConnector(object):
    _redis_client: RedisClientTuple = None
    _redis_auth_int_client: aioredis.Redis = None
    _redis_auth_ext_client: aioredis.Redis = None

    @classmethod
    async def create_redis_client(cls):
        cls._redis_auth_int_client = await aioredis.create_redis_pool(
            REDIS_ADDRESS,
            db=REDIS_AUTH_INT_DB,
            encoding='utf-8',
            maxsize=25)
        cls._redis_auth_ext_client = await aioredis.create_redis_pool(
            REDIS_ADDRESS,
            db=REDIS_AUTH_EXT_DB,
            encoding='utf-8',
            maxsize=25)
        cls._redis_client = RedisClientTuple(cls._redis_auth_int_client, cls._redis_auth_ext_client)

    @classmethod
    async def close_redis_client(cls):
        cls._redis_auth_int_client.close()
        cls._redis_auth_ext_client.close()

    @classmethod
    async def get_redis_client(cls):
        if not cls._redis_client:
            await cls.create_redis_client()
        return cls._redis_client
