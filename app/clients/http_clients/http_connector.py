from aiohttp import TCPConnector, ClientSession


class HttpAsyncConnector(object):
    _aiohttp_connector: TCPConnector = None
    _aiohttp_session: ClientSession = None

    @classmethod
    async def create_http_client(cls):
        cls._aiohttp_connector = TCPConnector(
            ttl_dns_cache=3600,
            limit=50,
            enable_cleanup_closed=True)
        cls._aiohttp_session = ClientSession(
            connector=cls._aiohttp_connector)

    @classmethod
    async def close_http_client(cls):
        # closing session closes underlying connector as well
        await cls._aiohttp_session.close()

    @classmethod
    async def get_http_client(cls):
        if not cls._aiohttp_session:
            await cls.create_http_client()
        return cls._aiohttp_session
