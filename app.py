import logging
from datetime import datetime

import uvicorn

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response
from starlette.endpoints import HTTPEndpoint
from starlette.templating import Jinja2Templates
from starlette.background import BackgroundTask

from indexer.authority_indexer import create_authority_index

from updater.updater_status import UpdaterStatus
from updater.authority_updater import AuthorityUpdater
from updater.background_tasks import do_authority_update

from objects.bib import BibliographicRecordsChunk

from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST, PROD_PORT


templates = Jinja2Templates(directory='templates')

app = Starlette(debug=False, template_directory='templates')


# homepage
@app.route('/')
async def homepage(request):
    return templates.TemplateResponse('docs.html', {'request': request})


# bibs
# chunk of bibs - data.bn.org.pl wrapper - main endpoint
@app.route('/api/{identifier_type}/bibs')
class BibChunkEnrichedWithIds(HTTPEndpoint):
    async def get(self, request):
        identifier_type = request.path_params['identifier_type']
        bib_chunk_object = BibliographicRecordsChunk(
            request.query_params, local_auth_index, identifier_type)
        return Response(bib_chunk_object.xml_processed_chunk, media_type='application/xml')

# authorities
# single authority endpoint
@app.route('/api/authorities/{authority_id}')
class AuthoritySingle(HTTPEndpoint):
    async def get(self, request):
        authority_id = request.path_params['authority_id']
        response = local_auth_index[authority_id]
        return PlainTextResponse(str(response))

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
    auth_marc = 'nlp_database/test/authorities_test_100.mrc'

    local_auth_index = create_authority_index(auth_marc)

    # create updaters and updater_status
    auth_updater = AuthorityUpdater()
    updater_status = UpdaterStatus(datetime.utcnow())

    if IS_LOCAL:
        uvicorn.run(app, host=LOC_HOST, port=LOC_PORT)
    else:
        uvicorn.run(app, host=PROD_HOST, port=PROD_PORT)
