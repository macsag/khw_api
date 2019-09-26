import logging
from datetime import datetime

import uvicorn

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, Response, RedirectResponse
from starlette.endpoints import HTTPEndpoint
from starlette.templating import Jinja2Templates
from starlette.background import BackgroundTask

from indexer.authority_indexer import create_authority_index
from indexer.bib_indexer import create_bib_index

from updater.updater_status import UpdaterStatus
from updater.authority_updater import AuthorityUpdater
import updater.bib_updater
from updater.background_tasks import do_authority_update, do_bib_update

from objects.bib import BibliographicRecordsChunk

from utils.marc_utils import get_single_marc_authority_record_from_data_bn, read_marc_from_binary
from utils.descriptor_converter import convert_descriptor, descriptor_types
from utils.search_utils import search_in_data_bn, get_fields_from_json, get_descriptor_type_from_json

from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST, PROD_PORT


templates = Jinja2Templates(directory='templates')

app = Starlette(debug=False, template_directory='templates')


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


@app.route('/authorities/{authority_id}')
class AuthoritySingleHtml(HTTPEndpoint):
    async def get(self, request):
        authority_id = request.path_params['authority_id']
        raw = get_single_marc_authority_record_from_data_bn(authority_id)
        marc_object = read_marc_from_binary(raw)
        converted = convert_descriptor(marc_object, descriptor_types)
        response = ([str(f) for f in marc_object.get_fields()], raw, converted)
        print(response)
        return templates.TemplateResponse('authority_single.html', {'request': request, 'response': response})


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


@app.route('/search')
class SearchEndpoint(HTTPEndpoint):
    async def get(self, request):
        search_phrase = request.query_params['q']
        if get_single_marc_authority_record_from_data_bn(search_phrase):
            return RedirectResponse(url=f'/authorities/{search_phrase}')
        else:
            search_result = search_in_data_bn(search_phrase)
            if search_result['authorities']:
                records_in_json = search_result['authorities']
                response = []
                for rcd in records_in_json:
                    pref_name = get_fields_from_json(rcd, '150')
                    alt_names = get_fields_from_json(rcd, '450')
                    descr_type = get_descriptor_type_from_json(rcd, descriptor_types)
                    response.append({'pref_name': pref_name, 'alt_names': alt_names, 'type': descr_type}, )
                return templates.TemplateResponse('search.html', {'request': request, 'response': response})
            else:
                pass


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
    bib_marc = 'nlp_database/test/bibs_test_100.mrc'

    local_auth_index = create_authority_index(auth_marc)
    local_bib_index = create_bib_index(bib_marc)

    # create updaters and updater_status
    bib_updater = updater.bib_updater.BibUpdater()
    auth_updater = AuthorityUpdater()
    updater_status = UpdaterStatus(datetime.utcnow())


    if IS_LOCAL:
        uvicorn.run(app, host=LOC_HOST, port=LOC_PORT)
    else:
        uvicorn.run(app, host=PROD_HOST, port=PROD_PORT)
