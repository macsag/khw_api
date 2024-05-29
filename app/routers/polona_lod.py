from aiohttp import ClientSession
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from clients.http_clients.http_connector import HttpAsyncConnector
from clients.redis_clients.redis_connector import RedisClientTuple, RedisAsyncConnector
from models.polona_lod import PolonaLodOutV2
from objects_business_logic.polona_lod import PolonaLodHandler
from utils.marc_utils import normalize_nlp_id_bib


router = APIRouter(tags=['polona_lod'])
templates = Jinja2Templates(directory='templates')


@router.get('/polona-lod/{bib_nlp_id}', response_class=HTMLResponse)
async def get_polona_lod_html(
        bib_nlp_id: str,
        request: Request,
        aiohttp_client: ClientSession = Depends(HttpAsyncConnector.get_http_client),
        redis_client: RedisClientTuple = Depends(RedisAsyncConnector.get_redis_client)):
    bib_nlp_id = normalize_nlp_id_bib(bib_nlp_id)

    polona_handler = PolonaLodHandler(aiohttp_client,
                                      redis_client,
                                      bib_nlp_id)
    polona_json = await polona_handler.get_polona_json()

    return templates.TemplateResponse(
        request=request,
        name='polona-lod.html',
        context={'bib_nlp_id': bib_nlp_id,
                 'polona_json': polona_json})


@router.get('/api/v2/polona-lod/{bib_nlp_id}', response_model=PolonaLodOutV2)
async def get_polona_lod_v2(
        bib_nlp_id: str,
        aiohttp_client: ClientSession = Depends(HttpAsyncConnector.get_http_client),
        redis_client: RedisClientTuple = Depends(RedisAsyncConnector.get_redis_client)):
    bib_nlp_id = normalize_nlp_id_bib(bib_nlp_id)

    polona_handler = PolonaLodHandler(aiohttp_client,
                                      redis_client,
                                      bib_nlp_id)
    polona_json_v2 = await polona_handler.get_polona_json_v2()

    return polona_json_v2
