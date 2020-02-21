import json
from datetime import datetime

import uvicorn
import aioredis
import aiohttp

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response, JSONResponse
from starlette.endpoints import HTTPEndpoint
from starlette.templating import Jinja2Templates
from starlette.background import BackgroundTask

from updater.updater_status import UpdaterStatus
from updater.authority_updater import AuthorityUpdater
from updater.background_tasks import do_authority_update
from objects.bib import BibliographicRecordsChunk
from objects.polona_lod import PolonaLodRecord
from utils.marc_utils import normalize_nlp_id

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
    # setup logging
    # and once again on the app startup
    logconfig_dict = read_logging_config('applog/logging.yml')
    setup_logging(logconfig_dict)

    # setup async redis connection pools
    global conn_auth_int
    global conn_auth_ext
    conn_auth_int = await aioredis.create_redis_pool('redis://localhost', db=0, encoding='utf-8', maxsize=200)
    conn_auth_ext = await aioredis.create_redis_pool('redis://localhost', db=1, encoding='utf-8', maxsize=200)

    # setup async aiohttp connection pool
    global aiohttp_session
    aiohttp_session = aiohttp.ClientSession()

    # create updaters and updater_status
    global auth_updater
    global updater_status
    auth_updater = AuthorityUpdater()
    updater_status = UpdaterStatus(datetime.utcnow())


# homepage
@app.route('/')
async def homepage(request):
    return templates.TemplateResponse('docs.html', {'request': request})


# bibs
# chunk of bibs - data.bn.org.pl wrapper - main endpoint
@app.route('/api/{identifier_type}/bibs')
class BibsChunkEnrichedWithIds(HTTPEndpoint):
    async def get(self, request):
        identifier_type = request.path_params['identifier_type']

        bib_chunk_object = await BibliographicRecordsChunk(aiohttp_session,
                                                           conn_auth_int,
                                                           conn_auth_ext,
                                                           request.query_params,
                                                           identifier_type)
        resp_content = bib_chunk_object.xml_processed_chunk

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
                joined_dict.update({auth: json.loads(auth_ids)})
            else:
                joined_dict.update({auth: auth_ids})
        return JSONResponse(joined_dict)


# polona-lod
# html endpoint for polona.pl
# aggregates authority external ids for single bib record
# and presents them with some additional context from NLP descriptors
@app.route('/polona-lod/{bib_nlp_id}')
class PolonaLodFront(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = normalize_nlp_id(request.path_params['bib_nlp_id'])
        polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
        polona_json = polona_back.get_json()
        return templates.TemplateResponse('polona-lod.html', {'request': request,
                                                              'bib_nlp_id': bib_nlp_id,
                                                              'polona_json': polona_json})


# json endpoint for polona.pl
@app.route('/api/polona-lod/{bib_nlp_id}')
class PolonaLodAPI(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = normalize_nlp_id(request.path_params['bib_nlp_id'])
        polona_back = await PolonaLodRecord(bib_nlp_id, aiohttp_session, conn_auth_int, conn_auth_ext)
        polona_json = polona_back.get_json()
        return JSONResponse(polona_json)


# updater
# schedule update
@app.route('/updater/{index_to_update}')
class IndexUpdater(HTTPEndpoint):
    async def get(self, request):
        index_to_update = request.path_params['index_to_update']

        if index_to_update == 'authorities':
            if updater_status.update_in_progress:
                return PlainTextResponse("Aktualizacja w toku. Spróbuj za chwilę.")
            else:
                task = BackgroundTask(do_authority_update, auth_updater, conn_auth_int, updater_status)
                return PlainTextResponse("Rozpoczęto aktualizację.", background=task)


# get updater status
@app.route('/updater/status/')
class UpdaterStatusView(HTTPEndpoint):
    async def get(self, request):
        return JSONResponse({'update_in_progress': updater_status.update_in_progress,
                             'last_update': str(updater_status.last_auth_update)})


if __name__ == '__main__':

    if not IS_LOCAL:
        uvicorn.run('app:app', host=LOC_HOST, port=LOC_PORT)
    else:
        uvicorn.run('app:app', host=PROD_HOST, port=PROD_PORT, workers=3)
