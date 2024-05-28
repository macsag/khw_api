from aiohttp import ClientSession
from fastapi import APIRouter, Depends, Request, Response

from models.bibs import IdentifierTypeName
from objects_business_logic.bibs import BibliographicRecordsHandler
from clients.http_clients.http_connector import HttpAsyncConnector
from clients.redis_clients.redis_connector import RedisClientTuple, RedisAsyncConnector


router = APIRouter(tags=['bibs'])


@router.get('/api/{identifier_type}/bibs')
async def get_single_or_multiple_bibliographic_records(
        identifier_type: IdentifierTypeName,
        request: Request,
        http_client: ClientSession = Depends(HttpAsyncConnector.get_http_client),
        redis_client: RedisClientTuple = Depends(RedisAsyncConnector.get_redis_client)):
    """
    Get single authority or multiple authorities using parameters as available in data.bn.org.pl API.
    
    This endpoint acts as a pass-through endpoint to data.bn.org.pl API, but enriches the data
    (specifically the "authorized access points" aka descriptors) with National Library of Poland unique identifiers
    (or MMS ID).
    
    You will get the response, which mimics the original data.bn.org.pl API data model (MARCXML).
    """
    # we have to access the request directly, because query params are dynamic and defined in the original
    # data.bn.org.pl API enpoint
    bibs_handler = BibliographicRecordsHandler(http_client,
                                               redis_client,
                                               request.query_params,
                                               identifier_type)
    bibs = await bibs_handler.get_bibs()

    # we can't use pydantic model, because we want the response to be xml
    return Response(content=bibs, media_type='application/xml')
