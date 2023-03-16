import json

import uvicorn
import aioredis
import aiohttp

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response, JSONResponse
from starlette.endpoints import HTTPEndpoint
from starlette.templating import Jinja2Templates
from starlette.background import BackgroundTask

from updater.authority_updater import AuthorityUpdater
from updater.background_tasks import do_authority_update
from objects.bib import BibliographicRecordsChunk
from objects.authority import AuthorityRecordsChunk
from objects.polona_lod import PolonaLodRecord
from utils.marc_utils import normalize_nlp_id_bib, convert_nlp_id_auth_to_sierra_format

from applog.utils import read_logging_config, setup_logging
from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST, PROD_PORT


# setup logging
# due to some unexpected behaviour of unicorn logging, logging is being set up twice on purpose!
# before the app startup
logconfig_dict = read_logging_config('applog/logging.yml')
setup_logging(logconfig_dict)

templates = Jinja2Templates(directory='templates')
app = Starlette(debug=False, template_directory='templates')


@app.on_event("startup")
async def startup():
    # setup logging once again on the app startup
    logconfig_dict = read_logging_config('applog/logging.yml')
    setup_logging(logconfig_dict)

    # setup async redis connection pools
    global conn_auth_int
    global conn_auth_ext
    conn_auth_int = await aioredis.create_redis_pool('redis://localhost', db=8, encoding='utf-8', maxsize=50)
    conn_auth_ext = await aioredis.create_redis_pool('redis://localhost', db=9, encoding='utf-8', maxsize=50)

    # setup async aiohttp connection pool
    global aiohttp_connector
    aiohttp_connector = aiohttp.TCPConnector(ttl_dns_cache=3600, limit=50, enable_cleanup_closed=True)
    global aiohttp_session
    aiohttp_session = aiohttp.ClientSession(connector=aiohttp_connector)

    # create updaters and updater_status
    global auth_updater
    auth_updater = AuthorityUpdater()


# homepage
@app.route('/')
async def homepage(request):
    return templates.TemplateResponse('docs.html', {'request': request})


# bibs
# chunk of bibs - data.bn.org.pl wrapper - main endpoint
@app.route('/api/{identifier_type}/bibs')
class BibsChunkEnrichedWithIds(HTTPEndpoint):
    async def get(self, request):
        bib_chunk_object = await BibliographicRecordsChunk(aiohttp_session,
                                                           conn_auth_int,
                                                           conn_auth_ext,
                                                           request.query_params,
                                                           request.path_params['identifier_type'])
        resp_content = bib_chunk_object.xml_processed_chunk

        return Response(resp_content, media_type='application/xml')


@app.route('/api/{identifier_type}/authorities')
class AuthoritiesChunkEnrichedWithIds(HTTPEndpoint):
    async def get(self, request):
        authorities_chunk_object = await AuthorityRecordsChunk(aiohttp_session,
                                                               conn_auth_int,
                                                               conn_auth_ext,
                                                               request.query_params,
                                                               request.path_params['identifier_type'])
        resp_content = authorities_chunk_object.xml_processed_chunk

        return Response(resp_content, media_type='application/xml')


# authorities
# returns authorities with internal and external ids endpoint (single or more) in json
@app.route('/api/authorities/{authority_ids}')
class AuthoritiesChunkWithExternalIds(HTTPEndpoint):
    async def get(self, request):
        authority_ids = [auth_id for auth_id in request.path_params['authority_ids'].split(',')]
        resp = await conn_auth_int.mget(*authority_ids)

        joined_dict = {}
        for auth, auth_ids in zip(authority_ids, resp):
            if auth_ids:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_internal', {}).update(json.loads(auth_ids))
            else:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_internal', None)

        authority_ids_transformed_to_sierra_format = {auth_id: convert_nlp_id_auth_to_sierra_format(auth_id) for auth_id in authority_ids}
        resp_2 = await conn_auth_ext.mget(*list(authority_ids_transformed_to_sierra_format.values()))

        for auth, auth_ids in zip(list(authority_ids_transformed_to_sierra_format.keys()), resp_2):
            if auth_ids:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_external', {}).update(json.loads(auth_ids))
            else:
                joined_dict.setdefault(auth, {}).setdefault('ids_from_external', None)

        return JSONResponse(joined_dict)


# polona-lod
# html endpoint for polona.pl
# aggregates authority external ids for single bib record
# and presents them with some additional context from NLP descriptors
@app.route('/polona-lod/{bib_nlp_id}')
class PolonaLodFront(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = normalize_nlp_id_bib(request.path_params['bib_nlp_id'])
        polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
        polona_json = polona_back.get_json()
        return templates.TemplateResponse('polona-lod.html', {'request': request,
                                                              'bib_nlp_id': bib_nlp_id,
                                                              'polona_json': polona_json})


# json endpoint for polona.pl
@app.route('/api/polona-lod/{bib_nlp_id}')
class PolonaLodAPI(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = normalize_nlp_id_bib(request.path_params['bib_nlp_id'])
        polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
        polona_json = polona_back.get_json()
        return JSONResponse(polona_json)


# json endpoint for polona.pl v2
@app.route('/api/v2/polona-lod/{bib_nlp_id}')
class PolonaLodV2API(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = normalize_nlp_id_bib(request.path_params['bib_nlp_id'])
        polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
        polona_json = polona_back.get_json_v2()
        return JSONResponse(polona_json)


# updater
# schedule update
@app.route('/updater/authorities')
class IndexUpdater(HTTPEndpoint):
    async def get(self, request):
        if auth_updater.update_in_progress:
            return PlainTextResponse("Aktualizacja w toku. Spróbuj za chwilę.")
        else:
            task = BackgroundTask(do_authority_update, auth_updater, aiohttp_session, conn_auth_int)
            return PlainTextResponse("Rozpoczęto aktualizację.", background=task)


# get updater status
@app.route('/updater/status/')
class UpdaterStatusView(HTTPEndpoint):
    async def get(self, request):
        return JSONResponse({'update_in_progress': auth_updater.update_in_progress,
                             'last_update': str(auth_updater.last_auth_update)})


if __name__ == '__main__':

    if IS_LOCAL:
        uvicorn.run('app:app', host=LOC_HOST, port=LOC_PORT)
    else:
        uvicorn.run('app:app', host=PROD_HOST, port=PROD_PORT, workers=3)
