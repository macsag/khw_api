from aiohttp import ClientSession
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from objects_business_logic.polona_lod import PolonaLodHandler
from clients.http_clients.http_connector import HttpAsyncConnector
from utils.marc_utils import normalize_nlp_id_bib
from clients.redis_clients.redis_connector import RedisClientTuple, RedisAsyncConnector

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


# json endpoint for polona.pl
@router.get('/api/polona-lod/{bib_nlp_id}')
async def get(self, request):
    bib_nlp_id = normalize_nlp_id_bib(request.path_params['bib_nlp_id'])
    polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
    polona_json = polona_back.get_json()
    return JSONResponse(polona_json)


# json endpoint for polona.pl v2
@router.get('/api/v2/polona-lod/{bib_nlp_id}')
async def get(self, request):
    bib_nlp_id = normalize_nlp_id_bib(request.path_params['bib_nlp_id'])
    polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
    polona_json = polona_back.get_json_v2()
    return JSONResponse(polona_json)