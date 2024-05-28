import os
from typing import Optional

from pymarc import MARCReader, Record

from utils.marc_utils import prepare_name_for_indexing, process_record
from utils.indexer_consts import FIELDS_TO_CHECK

FIELDS_TO_NAT_LANG = {'100': 'Twórca/współtwórca',
                      '110': 'Twórca/współtwórca',
                      '111': 'Twórca/współtwórca',
                      '130': 'Tytuł ujednolicony',
                      '730': 'Tytuł ujednolicony',
                      '700': 'Twórca/współtwórca',
                      '710': 'Twórca/współtwórca',
                      '711': 'Twórca/współtwórca',
                      '600': 'Temat: osoba',
                      '610': 'Temat: instytucja',
                      '611': 'Temat: wydarzenie',
                      '630': 'Temat: dzieło',
                      '650': 'Temat',
                      '651': 'Temat: miejsce',
                      '655': 'Rodzaj/gatunek',
                      '658': 'Dziedzina/ujęcie',
                      '380': 'Forma/typ',
                      '388': 'Czas powstania dzieła/realizacji',
                      '385': 'Odbiorca',
                      '386': 'Przynależność kulturowa',
                      '648': 'Temat: czas',
                      '830': 'Seria/tytuł ujednolicony'}


DATA_BN_BIBS_BASE_ADDRESS = os.getenv("DATA_BN_BIBS_BASE_ADDRESS")


class PolonaLodHandler(object):
    def __init__(self, aiohttp_client, redis_client, bib_record_nlp_id: str):
        self._aiohttp_client = aiohttp_client
        self._conn_auth_int = redis_client.auth_int
        self._conn_auth_ext = redis_client.auth_ext

        self._bib_record_nlp_id = bib_record_nlp_id

        self._bib_record_raw_marc_iso_bytes = None
        self._bib_record_parsed_pymarc_object = None
        self._extracted_authorities_raw = None

        self.status = {'error': False, 'code': 200, 'message': ''}

    async def _get_bib_record_raw_marc_iso_bytes(self) -> Optional[bytes]:
        query = f'{DATA_BN_BIBS_BASE_ADDRESS}.marc?id={self._bib_record_nlp_id}'

        async with self._aiohttp_client.get(query) as response:
            if response.status == 200:
                return await response.read()
            if response.status == 404:
                self.status['error'] = True
                self.status['code'] = 404
                self.status['message'] = ''
            else:
                return None

    @staticmethod
    def _read_single_marc_record_from_binary(bib_record_raw_marc_iso_bytes: bytes) -> Optional[Record]:
        rcds = []

        marc_rdr = MARCReader(bib_record_raw_marc_iso_bytes,
                                  to_unicode=True,
                                  force_utf8=True,
                                  utf8_handling='ignore',
                                  permissive=True)
        for rcd in marc_rdr:
            rcds.append(rcd)

        if rcds:
            return rcds[0]
        else:
            return None

    @staticmethod
    def _get_authorities_ids_from_internal_db(term_to_search: str, processed_record: Record) -> Optional[dict]:
        all_ids = {}

        if term_to_search in processed_record:
            i_ids = processed_record.get(term_to_search).get('internal_ids')
            if i_ids:
                all_ids.update(i_ids)
            e_ids = processed_record.get(term_to_search).get('external_ids')
            if e_ids:
                all_ids.update(e_ids)

        return all_ids if all_ids else None

    async def _extract_selected_authorities_from_record(
            self,
            bib_record_parsed_pymarc_object: Record) -> Optional[dict]:
        extracted_authorities = {}

        if bib_record_parsed_pymarc_object:
            processed_record = await process_record(bib_record_parsed_pymarc_object,
                                                    'all_ids',
                                                    self._conn_auth_int,
                                                    self._conn_auth_ext,
                                                    polona=True)

            for marc_field_and_subfields in FIELDS_TO_CHECK:
                fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

                if fld in self._bib_record_parsed_pymarc_object:
                    raw_objects_flds_list = self._bib_record_parsed_pymarc_object.get_fields(fld)

                    for raw_fld in raw_objects_flds_list:
                        term_to_search = prepare_name_for_indexing(
                            ' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                        single_extracted_authority = self._get_authorities_ids_from_internal_db(term_to_search,
                                                                                                processed_record)

                        if single_extracted_authority:
                            extracted_authorities.setdefault(FIELDS_TO_NAT_LANG.get(fld),
                                                             {}).setdefault(term_to_search,
                                                                            {}).update(single_extracted_authority)

            return extracted_authorities if extracted_authorities else None
        else:
            return None

    @staticmethod
    def _convert_authorities_to_polona_json(extracted_authorities_raw: dict) -> Optional[dict]:
        converted_json = {}

        if extracted_authorities_raw:
            for auth_role, auth in extracted_authorities_raw.items():
                for auth_name, auth_ids in auth.items():
                    heading = auth_ids.get('heading')
                    for auth_id_type, auth_id in auth_ids.items():
                        if auth_id_type == 'nlp_id':
                            to_update = {'display': auth_id,
                                         'link': f'https://dbn.bn.org.pl/descriptor-details/{auth_id}'}
                            converted_json.setdefault(auth_role,
                                                      {}).setdefault(heading,
                                                                     {}).setdefault('Identyfikator BN',
                                                                                    {}).update(to_update)
                        if auth_id_type == 'viaf_uri' and auth_id:
                            to_update = {'display': auth_id,
                                         'link': auth_id}
                            converted_json.setdefault(auth_role,
                                                      {}).setdefault(heading,
                                                                     {}).setdefault('Identyfikator VIAF (URI)',
                                                                                    {}).update(to_update)
                        if auth_id_type == 'wikidata_uri' and auth_id:
                            to_update = {'display': auth_id,
                                         'link': auth_id}
                            converted_json.setdefault(auth_role,
                                                      {}).setdefault(heading,
                                                                     {}).setdefault('Identyfikator Wikidata (URI)',
                                                                                    {}).update(to_update)
                        if auth_id_type == 'coords' and auth_id:
                            parsed_coords = auth_id.split(',')
                            lat = parsed_coords[1]
                            lon = parsed_coords[0]

                            to_update = {'display': f'długość: {lon}, szerokość: {lat}',
                                         'link': f'http://www.openstreetmap.org/?mlat={lat}&mlon={lon}&zoom=6'}
                            converted_json.setdefault(auth_role,
                                                      {}).setdefault(heading,
                                                                     {}).setdefault('Współrzędne geograficzne',
                                                                                    {}).update(to_update)
                        if auth_id_type == 'geonames_uri' and auth_id:
                            to_update = {'display': auth_id,
                                         'link': auth_id}
                            converted_json.setdefault(auth_role,
                                                      {}).setdefault(heading,
                                                                     {}).setdefault('Identyfikator Geonames (URI)',
                                                                                    {}).update(to_update)
        return converted_json

    async def get_polona_json(self) -> Optional[dict]:
        bib_record_raw_marc_iso_bytes = await self._get_bib_record_raw_marc_iso_bytes()

        if not bib_record_raw_marc_iso_bytes:
            return {}

        bib_record_parsed_pymarc_object = self._read_single_marc_record_from_binary(bib_record_raw_marc_iso_bytes)

        if not bib_record_parsed_pymarc_object:
            return {}

        extracted_authorities_raw = await self._extract_selected_authorities_from_record(bib_record_parsed_pymarc_object)
        extracted_authorities_converted = self._convert_authorities_to_polona_json(extracted_authorities_raw)

        return extracted_authorities_converted

    async def get_polona_json_v2(self):
        extracted_authorities_converted = await self.get_polona_json()
        return self._convert_authorities_to_polona_json_v2(extracted_authorities_converted)

    @staticmethod
    def _convert_authorities_to_polona_json_v2(extracted_authorities_converted: dict) -> dict:
        descriptors = []

        for auth_role, descriptor in extracted_authorities_converted.items():
            descriptors_with_auth_role = {'name': auth_role}
            subjects = []

            for heading, id_object in descriptor.items():
                subject = {'name': heading}
                identifiers = []

                for id_type, id_details in id_object.items():
                    identifiers.append({'type': id_type,
                                        'display': id_details.get('display'),
                                        'link': id_details.get('link')})

                subject['identifiers'] = identifiers
                subjects.append(subject)

            descriptors_with_auth_role['subjects'] = subjects
            descriptors.append(descriptors_with_auth_role)

        return {'descriptors': descriptors}
