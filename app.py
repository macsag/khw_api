import logging
from datetime import datetime

import uvicorn

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response, JSONResponse
from starlette.endpoints import HTTPEndpoint
from starlette.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.staticfiles import StaticFiles

from indexer.authority_indexer import create_authority_index
from indexer.authority_external_ids_indexer import AuthorityExternalIdsIndex

from updater.updater_status import UpdaterStatus
from updater.authority_updater import AuthorityUpdater
from updater.background_tasks import do_authority_update

from objects.bib import BibliographicRecordsChunk
from objects.authority import AuthorityRecordsChunk
from objects.polona_lod import PolonaLodRecord

from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST, PROD_PORT


templates = Jinja2Templates(directory='templates')

app = Starlette(debug=True, template_directory='templates')
app.mount('/static', StaticFiles(directory='static'), name='static')


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
        bib_chunk_object = BibliographicRecordsChunk(request.query_params, local_auth_index,
                                                     identifier_type, local_auth_external_ids_index)
        return Response(bib_chunk_object.xml_processed_chunk, media_type='application/xml')


# authorities
# single authority endpoint
@app.route('/api/authorities/{authority_ids}')
class AuthoritiesChunkWithExternalIds(HTTPEndpoint):
    async def get(self, request):
        authority_ids = request.path_params['authority_ids']
        authorities_chunk_object = AuthorityRecordsChunk(authority_ids, local_auth_index, local_auth_external_ids_index)
        return JSONResponse(authorities_chunk_object.json_processed_chunk)


# polona-lod
# html endpoint for polona.pl
# aggregates authority external ids for single bib record
# and presents them with some additional context from NLP descriptors and wikidata SPARQL endpoint
@app.route('/polona-lod/{bib_nlp_id}')
class PolonaLodFront(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = request.path_params['bib_nlp_id']
        polona_back = PolonaLodRecord(bib_nlp_id, local_auth_index, local_auth_external_ids_index)
        polona_json = polona_back.get_json()
        return templates.TemplateResponse('polona-lod.html', {'request': request,
                                                              'bib_nlp_id': bib_nlp_id,
                                                              'polona_json': polona_json})

# json endpoint for polona.pl
@app.route('/api/polona-lod/{bib_nlp_id}')
class PolonaLodAPI(HTTPEndpoint):
    async def get(self, request):
        bib_nlp_id = request.path_params['bib_nlp_id']
        polona_back = PolonaLodRecord(bib_nlp_id, local_auth_index, local_auth_external_ids_index)
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
                task = BackgroundTask(do_authority_update, auth_updater, local_auth_index, updater_status)
                return PlainTextResponse("Rozpoczęto aktualizację.", background=task)


# get updater status
@app.route('/updater/status/')
class UpdaterStatusView(HTTPEndpoint):
    async def get(self, request):
        return PlainTextResponse(f'Aktualizacja r. wzorcowych w toku: {str(updater_status.update_in_progress)}\n'
                                 f'Ostatnia aktualizacja: {str(updater_status.last_auth_update)}\n')


if __name__ == '__main__':
    # set logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    file_fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    root = logging.getLogger()

    fhandler = logging.FileHandler('khw_log.log', 'a', encoding='utf-8')
    fhandler.setFormatter(file_fmt)

    root.addHandler(fhandler)

    # set index source files
    auth_marc = 'nlp_database/production/authorities-all.marc'

    local_auth_index = create_authority_index(auth_marc)
    local_auth_external_ids_index = AuthorityExternalIdsIndex(geonames=True,
                                                              wikidata=True,
                                                              orcid=True)

    # create updaters and updater_status
    auth_updater = AuthorityUpdater()
    updater_status = UpdaterStatus(datetime.utcnow())

    if IS_LOCAL:
        uvicorn.run(app, host=LOC_HOST, port=LOC_PORT)
    else:
        uvicorn.run(app, host=PROD_HOST, port=PROD_PORT)
