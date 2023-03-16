from typing import Optional

from pymarc import MARCReader, Record

from utils.marc_utils import prepare_name_for_indexing, process_record
from config.indexer_config import FIELDS_TO_CHECK

FIELDS_TO_NAT_LANG = {'100': 'Twórca/współtwórca', '110': 'Twórca/współtwórca', '111': 'Twórca/współtwórca',
                      '130': 'Tytuł ujednolicony', '730': 'Tytuł ujednolicony',
                      '700': 'Twórca/współtwórca', '710': 'Twórca/współtwórca', '711': 'Twórca/współtwórca',
                      '600': 'Temat: osoba', '610': 'Temat: instytucja', '611': 'Temat: wydarzenie',
                      '630': 'Temat: dzieło', '650': 'Temat', '651': 'Temat: miejsce',
                      '655': 'Rodzaj/gatunek', '658': 'Dziedzina/ujęcie', '380': 'Forma/typ',
                      '388': 'Czas powstania dzieła/realizacji', '385': 'Odbiorca',
                      '386': 'Przynależność kulturowa', '648': 'Temat: czas', '830': 'Seria/tytuł ujednolicony'}


class PolonaLodRecord(object):
    def __init__(self, bib_record_nlp_id: str, aiohttp_session, conn_auth_int, conn_auth_ext):
        self._bib_record_nlp_id = bib_record_nlp_id
        self._bib_record_raw_marc_iso_bytes = None
        self._bib_record_parsed_pymarc_object = None
        self._extracted_authorities_raw = None
        self.status = {'error': False, 'code': 200, 'message': ''}

    async def get_bib_record_raw_marc_iso_bytes(self, aiohttp_session) -> Optional[bytes]:
        query = f'http://data.bn.org.pl/api/institutions/bibs.marc?id={self._bib_record_nlp_id}'

        async with aiohttp_session.get(query) as response:
            if response.status == 200:
                return await response.read()
            if response.status == 404:
                self.status['error'] = True
                self.status['code'] = 404
                self.status['message'] = ''
            else:
                return None

    def read_single_marc_record_from_binary(self) -> Optional[Record]:
        if self._bib_record_raw_marc_iso_bytes:
            marc_rdr = MARCReader(self._bib_record_raw_marc_iso_bytes,
                                  to_unicode=True,
                                  force_utf8=True,
                                  utf8_handling='ignore',
                                  permissive=True)
            for rcd in marc_rdr:
                return rcd
        else:
            return None

    async def extract_selected_authorities_from_record(self, conn_auth_int, conn_auth_ext):
        extracted_authorities = {}

        if self._bib_record_parsed_pymarc_object:
            processed_record = await process_record(self._bib_record_parsed_pymarc_object,
                                                    conn_auth_int,
                                              'all_ids',
                                                    conn_auth_ext,
                                                    polona=True)

            for marc_field_and_subfields in FIELDS_TO_CHECK:
                fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

                if fld in self._bib_record_parsed_pymarc_object:
                    raw_objects_flds_list = self._bib_record_parsed_pymarc_object.get_fields(fld)

                    for raw_fld in raw_objects_flds_list:
                        term_to_search = prepare_name_for_indexing(
                            ' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                        single_extracted_authority = self.get_authorities_ids_from_internal_db(term_to_search,
                                                                                               processed_record)

                        if single_extracted_authority:
                            extracted_authorities.setdefault(FIELDS_TO_NAT_LANG.get(fld),
                                                             {}).setdefault(term_to_search,
                                                                            {}).update(single_extracted_authority)


            return extracted_authorities if extracted_authorities else None
        else:
            return None

    @staticmethod
    def get_authorities_ids_from_internal_db(term_to_search, processed_record):
        all_ids = {}

        if term_to_search in processed_record:
            i_ids = processed_record.get(term_to_search).get('internal_ids')
            if i_ids:
                all_ids.update(i_ids)
            e_ids = processed_record.get(term_to_search).get('external_ids')
            if e_ids:
                all_ids.update(e_ids)

        return all_ids if all_ids else None

    def get_full_authority_records_from_internal_db(self):
        pass

    def convert_authorities_to_polona_json(self):
        converted_json = {}

        if self._extracted_authorities_raw:

            for auth_role, auth in self._extracted_authorities_raw.items():
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

    def get_json(self):
        return self._extracted_authorities_converted

    def get_authorities_from_bib_record(self):
        self.
        descriptors = []

        for auth_role, descriptor in self._extracted_authorities_converted.items():
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
