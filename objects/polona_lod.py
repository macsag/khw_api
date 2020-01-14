from typing import Optional

import requests
from pymarc import MARCReader, Record

from utils.marc_utils import prepare_name_for_indexing

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
    def __init__(self, bib_nlp_id, auth_index, auth_external_ids_index):
        self.bib_nlp_id = bib_nlp_id
        self.bib_bytes = self.get_single_marc_bib_record_from_data_bn()
        self.bib_pymarc_record = self.read_single_marc_record_from_binary()
        self.extracted_authorities = self.extract_selected_authorities_from_record(auth_index, auth_external_ids_index)
        self.converted_json = self.convert_authorities_to_polona_json()

    def get_single_marc_bib_record_from_data_bn(self) -> Optional[bytes]:
        query = f'http://data.bn.org.pl/api/bibs.marc?id={self.bib_nlp_id}'
        r = requests.get(query)
        return bytes(r.content) if r.status_code == 200 else None

    def read_single_marc_record_from_binary(self) -> Optional[Record]:
        if self.bib_bytes:
            marc_rdr = MARCReader(self.bib_bytes, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)
            for rcd in marc_rdr:
                return rcd
        else:
            return None

    def extract_selected_authorities_from_record(self, auth_index, auth_external_ids_index):
        extracted_authorities = {}

        if self.bib_pymarc_record:

            for marc_field_and_subfields in FIELDS_TO_CHECK:
                fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

                if fld in self.bib_pymarc_record:
                    raw_objects_flds_list = self.bib_pymarc_record.get_fields(fld)

                    for raw_fld in raw_objects_flds_list:
                        term_to_search = prepare_name_for_indexing(
                            ' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                        single_extracted_authority = self.get_authorities_ids_from_internal_db(term_to_search,
                                                                                               auth_index,
                                                                                               auth_external_ids_index)

                        if single_extracted_authority:
                            extracted_authorities.setdefault(FIELDS_TO_NAT_LANG.get(fld),
                                                             {}).setdefault(term_to_search,
                                                                            {}).update(single_extracted_authority)

            return extracted_authorities if extracted_authorities else None
        else:
            return None

    @staticmethod
    def get_authorities_ids_from_internal_db(term_to_search, auth_index, auth_external_ids_index):
        all_ids = {}

        if term_to_search in auth_index:
            nlp_id = list(auth_index.get(term_to_search).keys())[0]

            all_ids.update({'nlp_id': nlp_id,
                            'heading': list(auth_index.get(term_to_search).values())[0]["heading"],
                            'viaf_uri': list(auth_index.get(term_to_search).values())[0]["viaf_id"],
                            'coords': list(auth_index.get(term_to_search).values())[0]["coords"]})

            ext_ids = auth_external_ids_index.get_ids(nlp_id)

            if ext_ids:
                all_ids.update(ext_ids)

        return all_ids if all_ids else None

    def get_full_authority_records_from_internal_db(self):
        pass

    def convert_authorities_to_polona_json(self):
        converted_json = {}

        if self.extracted_authorities:

            for auth_role, auth in self.extracted_authorities.items():
                for auth_name, auth_ids in auth.items():
                    heading = auth_ids.get('heading')
                    for auth_id_type, auth_id in auth_ids.items():
                        if auth_id_type == 'nlp_id':
                            to_update = {'display': auth_id,
                                         'link': f'https://data.bn.org.pl/api/authorities.marcxml?id={auth_id}'}
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
        return self.converted_json
