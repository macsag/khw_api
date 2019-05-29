import logging
import sys

from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles
from starlette.responses import PlainTextResponse, Response, JSONResponse
from starlette.endpoints import HTTPEndpoint
from starlette.templating import Jinja2Templates
from starlette.background import BackgroundTask
import uvicorn

from indexer.authority_indexer import create_authority_index
from indexer.bib_indexer import create_bib_index

from updater.updater_status import UpdaterStatus
from updater.authority_updater import AuthorityUpdater
from updater.bib_updater import BibUpdater
from updater.background_tasks import do_authority_update, do_bib_update

from objects.bib import BibliographicRecordsChunk

from datetime import datetime


templates = Jinja2Templates(directory='templates')

app = Starlette(debug=True, template_directory='templates')
app.mount('/static', StaticFiles(directory='statics'), name='static')


# homepage
@app.route('/')
async def homepage(request):
    return templates.TemplateResponse('docs.html', {'request': request})


# bibs
# chunk of bibs - data.bn.org.pl wrapper
@app.route('/api/{identifier_type}/bibs')
class BibChunkEnrichedWithIds(HTTPEndpoint):
    async def get(self, request):
        identifier_type = request.path_params['identifier_type']
        bib_chunk_object = BibliographicRecordsChunk(
            request.query_params, local_auth_index, local_bib_index, identifier_type)
        return Response(bib_chunk_object.xml_processed_chunk, media_type='application/xml')


# bibs
# id resolver
@app.route('/api/id_resolver/bibs/{bib_id}')
class BibIdResolver(HTTPEndpoint):
    async def get(self, request):
        bib_id = request.path_params['bib_id']
        resolved_bib = resolver_bib_index.get(bib_id)
        return JSONResponse(resolved_bib)


# authorities
# id resolver
#@app.route('/id_resolver/authorities/{authority_id}')
#class AuthorityIdResolver(HTTPEndpoint):
#    async def get(self, request):
#        authority_id = request.path_params['authority_id']
#        resolved_authority = resolver_authority_index.get(authority_id)
#        return JSONResponse(resolved_authority)


@app.route('/api/bibs/{bib_id}')
class BibSingle(HTTPEndpoint):
    async def get(self, request):
        bib_id = request.path_params['bib_id']
        response = local_bib_index[bib_id]
        return PlainTextResponse(str(response))


@app.route('/api/authorities/{authority_id}')
class AuthoritySingle(HTTPEndpoint):
    async def get(self, request):
        authority_id = request.path_params['authority_id']
        response = local_auth_index[authority_id]
        return PlainTextResponse(str(response))


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
        if index_to_update == 'bibs':
            if updater_status.update_in_progress:
                return PlainTextResponse("Aktualizacja w toku. Spróbuj za chwilę.")
            else:
                task = BackgroundTask(do_bib_update, bib_updater, local_bib_index, updater_status)
                return PlainTextResponse("Rozpoczęto aktualizację.", background=task)


@app.route('/updater/status/')
class UpdaterStatusView(HTTPEndpoint):
    async def get(self, request):
        return PlainTextResponse(f'Aktualizacja w toku: {str(updater_status.update_in_progress)}\n'
                                 f'Ostatnia aktualizacja r. wzorcowych: {str(updater_status.last_auth_update)}\n'
                                 f'Ostatnia aktualizacja r. bibliograficznych: {str(updater_status.last_bib_update)}\n')


if __name__ == '__main__':
    logging.root.addHandler(logging.StreamHandler(sys.stdout))
    logging.root.setLevel(level=logging.DEBUG)


    # set index source files
    bib_marc = 'nlp_database/test/bibs-all.marc'
    auth_marc = 'nlp_database/test/authorities-all.marc'

    local_auth_index = create_authority_index(auth_marc)
    local_bib_index, resolver_bib_index = create_bib_index(bib_marc, resolver_index=True)

    # create updaters and updater_status
    bib_updater = BibUpdater()
    auth_updater = AuthorityUpdater()
    updater_status = UpdaterStatus(datetime.utcnow())

    uvicorn.run(app, host='127.0.0.1', port=8000)
