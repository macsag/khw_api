import itertools
import logging
import json
from pathlib import Path
from typing import List, Optional

import redis

from sqlite_clients.generic_client import GenericClient


logger = logging.getLogger(__name__)


class AuthorityExternalIdsIndex(object):
    def __init__(self, geonames=True, wikidata=True, orcid=True):
        self.geonames = geonames
        self.wikidata = wikidata
        self.orcid = orcid
        self.final_index = self.create_authority_external_ids_index()

    def create_authority_external_ids_index(self) -> dict:
        indexes_to_join = []

        if self.geonames:
            # geographic descriptors
            path_to_geo_geonames_db = Path.cwd() / 'sql_databases' / 'geographic_geonames_bn_filtered.db'
            geo_geonames_cl = GenericClient(str(path_to_geo_geonames_db),
                                   'SELECT bn__descr_nlp_id, result__geonames_id, score, max_score FROM results',
                                   'geonames_uri',
                                   id_processing_method=create_geonames_uri)

            geo_geonames_dict = geo_geonames_cl.create_index()
            indexes_to_join.append(geo_geonames_dict)

        if self.wikidata:
            # geographic descriptors
            path_to_geo_wikidata_db = Path.cwd() / 'sql_databases' / 'geographic_wikidata_bn_filtered.db'
            geo_wikidata_cl = GenericClient(str(path_to_geo_wikidata_db),
                                   'SELECT bn__descr_nlp_id, result__wkp_id, score, max_score FROM results',
                                   'wikidata_uri',
                                   id_processing_method=create_wikidata_uri)

            geo_wikidata_dict = geo_wikidata_cl.create_index()
            indexes_to_join.append(geo_wikidata_dict)

            # personal descriptors
            path_to_personal_wikidata_db = Path.cwd() / 'sql_databases' / 'personal_wikidata_bn.db'
            personal_wikidata_cl = GenericClient(str(path_to_personal_wikidata_db),
                                            'SELECT bn__descr_nlp_id, result__wkp_id, score, max_score FROM results',
                                            'wikidata_uri',
                                            id_processing_method=create_wikidata_uri)

            personal_wikidata_dict = personal_wikidata_cl.create_index()
            indexes_to_join.append(personal_wikidata_dict)

            # corporate descriptors
            path_to_corporate_wikidata_db = Path.cwd() / 'sql_databases' / 'corporate_wikidata_bn_filtered.db'
            corporate_wikidata_cl = GenericClient(str(path_to_corporate_wikidata_db),
                                                 'SELECT bn__descr_nlp_id, result__wkp_id, score, max_score FROM results',
                                                 'wikidata_uri',
                                                 id_processing_method=create_wikidata_uri)

            corporate_wikidata_dict = corporate_wikidata_cl.create_index()
            indexes_to_join.append(corporate_wikidata_dict)

            # subject descriptors
            path_to_subject_wikidata_db = Path.cwd() / 'sql_databases' / 'subject_wikidata_bn.db'
            subject_wikidata_cl = GenericClient(str(path_to_subject_wikidata_db),
                                                  'SELECT bn__descr_nlp_id, result__wkp_id, score, max_score FROM results',
                                                  'wikidata_uri',
                                                  id_processing_method=create_wikidata_uri)

            subject_wikidata_dict = subject_wikidata_cl.create_index()
            indexes_to_join.append(subject_wikidata_dict)

            # genre descriptors
            path_to_genre_wikidata_db = Path.cwd() / 'sql_databases' / 'genre_wikidata_bn.db'
            genre_wikidata_cl = GenericClient(str(path_to_genre_wikidata_db),
                                                'SELECT bn__descr_nlp_id, result__wkp_id, score, max_score FROM results',
                                                'wikidata_uri',
                                                id_processing_method=create_wikidata_uri)

            genre_wikidata_dict = genre_wikidata_cl.create_index()
            indexes_to_join.append(genre_wikidata_dict)

        if self.orcid:
            # personal descriptors
            path_to_personal_orcid_db = Path.cwd() / 'sql_databases' / 'personal_orcid_bn.db'
            personal_orcid_cl = GenericClient(str(path_to_personal_orcid_db),
                                                'SELECT bn__descr_nlp_id, result__orcid_id, score, max_score FROM results',
                                                'orcid_id',
                                                id_processing_method=None)

            personal_orcid_dict = personal_orcid_cl.create_index()
            indexes_to_join.append(personal_orcid_dict)

        final_index = self.join_indexes(indexes_to_join)
        return final_index

    def get_ids(self, nlp_id: str) -> Optional[str]:
        return self.final_index.get(self.transform_nlp_id(nlp_id))

    @staticmethod
    def transform_nlp_id(nlp_id: str) -> str:
        if len(nlp_id) == 14 and nlp_id[1] == '0':
            return f'a{calculate_check_digit(nlp_id[7:])}'
        else:
            return nlp_id

    @staticmethod
    def join_indexes(indexes_to_join: List[dict]) -> dict:
        final_dict = {}

        for index in indexes_to_join:
            for nlp_id, external_ids_dict in index.items():
                final_dict.setdefault(nlp_id, {}).update(external_ids_dict)

        return final_dict

    def index_in_redis(self):
        r = redis.Redis(db=1)
        r.flushdb()

        chunk_max_size = 1000
        keys_to_json = {k: json.dumps(v) for k, v in self.final_index.items()}
        chunks = [dict(itertools.islice(keys_to_json.items(), i, i + chunk_max_size)) for i in range(0, len(keys_to_json), chunk_max_size)]

        for chunk in chunks:
            r.mset(chunk)

        r.close()


def create_geonames_uri(geonames_id: str) -> str:
    return f'http://sws.geonames.org/{geonames_id}'


def create_wikidata_uri(wikidata_id: str) -> str:
    return f'http://www.wikidata.org/entity/{wikidata_id}'


def calculate_check_digit(record_id: str) -> str:
    char_sum = 0
    i = 2
    for character in record_id[::-1]:
        char_sum += int(character) * i
        i += 1
    remainder = char_sum % 11
    check_digit = str(remainder) if remainder != 10 else 'x'
    return record_id + check_digit
