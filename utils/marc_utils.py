import re
import json

from typing import Optional, Dict, List

import pymarc
import aioredis

from config.indexer_config import FIELDS_TO_CHECK, FIELDS_TO_CHECK_FOR_OMNIS


def prepare_name_for_indexing(descriptor_name: str) -> str:
    if descriptor_name:
        # 4.1 wszystko, co nie jest literą lub cyfrą zastępowane jest spacją
        descriptor_name = ''.join(char.replace(char, ' ') if not char.isalnum() else char for char in descriptor_name)

        # 4.2 wielokrotne białe znaki są redukowane do jednej spacji
        match = re.finditer(r'\s{2,}', descriptor_name)
        for m_object in match:
            descriptor_name = descriptor_name.replace(m_object.group(0), ' ')

        # 4.3 białe znaki z początku i końca są usuwane
        descriptor_name = descriptor_name.strip()

        # 4.4 wszystkie znaki podniesione do wielkich liter
        descriptor_name = descriptor_name.upper()

    return descriptor_name


def normalize_nlp_id_bib(nlp_id: str) -> str:
    if len(nlp_id) == 14 and nlp_id[1] in ['0', '1']:
        return nlp_id
    if len(nlp_id) == 7 and not nlp_id[0] == 'b':
        return f'b000000{nlp_id}'
    if len(nlp_id) == 8 and nlp_id[0] == 'b':
        return f'b000000{nlp_id[1:]}'
    if len(nlp_id) == 8 and not nlp_id[0] == 'b':
        return f'b000000{nlp_id[:-1]}'
    if len(nlp_id) == 9 and nlp_id[0] == 'b':
        return f'b000000{nlp_id[1:-1]}'
    else:
        return nlp_id


def convert_nlp_id_auth_to_sierra_format(nlp_id: str) -> Optional[str]:
    if len(nlp_id) == 14 and nlp_id[1] in ['0']:
        return f'a{calculate_check_digit(nlp_id[7:])}'
    else:
        return nlp_id


async def get_terms_to_search_and_references_to_raw_flds(marc_record: pymarc.Record,
                                                         for_omnis: bool = False) -> Dict[str, List[pymarc.Field]]:
    # create empty dict for results
    terms_fields_ids = {}

    # switch between fields to check
    fields_to_check = FIELDS_TO_CHECK_FOR_OMNIS if for_omnis else FIELDS_TO_CHECK

    # iterate over constant: fields to check in marc record
    for marc_field_and_subfields in fields_to_check:

        # get field tag and subfields tags
        fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

        # check if field present in marc record
        if fld in marc_record:

            # get list of raw field objects (pymarc.Field)
            raw_objects_flds_list = marc_record.get_fields(fld)

            # iterate over raw field objects
            for raw_fld in raw_objects_flds_list:

                # get term from raw field and normalize it
                term_to_search = prepare_name_for_indexing(
                    ' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                # create new entry in dict if necessary and/or append raw fld to list
                terms_fields_ids.setdefault(term_to_search, {}).setdefault('raw_flds', []).append(raw_fld)

    return terms_fields_ids


def transform_nlp_id(nlp_id: str) -> str:
    if len(nlp_id) == 14 and nlp_id[1] == '0':
        return f'a{calculate_check_digit(nlp_id[7:])}'
    else:
        return nlp_id


def calculate_check_digit(record_id: str) -> str:
    char_sum = 0
    i = 2
    for character in record_id[::-1]:
        char_sum += int(character) * i
        i += 1
    remainder = char_sum % 11
    check_digit = str(remainder) if remainder != 10 else 'x'
    return record_id + check_digit


async def process_record(marc_record: pymarc.Record,
                         conn_auth_int,
                         identifier_type: str,
                         conn_auth_ext,
                         polona: bool = False,
                         for_omnis: bool = False):
    """
    Main processing loop for adding authority identifiers to bibliographic record.
    """

    # get all terms to search for in redis index and get all references to raw flds with these terms
    terms_fields_ids = await get_terms_to_search_and_references_to_raw_flds(marc_record,
                                                                            for_omnis=for_omnis)

    # there are some cases, when there is nothing to resolve, so better check it
    if terms_fields_ids:
        # get all internal ids (meaning: from original NLP database via data.bn.org.pl) from redis index db=8
        internal_ids = await conn_auth_int.mget(*list(terms_fields_ids.keys()))

        # add internal ids to terms_fields_ids dict and prepare helper list of tuples for external ids redis query
        helper_list_for_ext_ids_query = []

        for term, int_ids in zip(list(terms_fields_ids.keys()), internal_ids):
            if int_ids:
                fields_ids = terms_fields_ids.get(term)
                in_json = json.loads(int_ids)
                fields_ids.setdefault('internal_ids', in_json)
                helper_list_for_ext_ids_query.append((term, transform_nlp_id(in_json.get('nlp_id'))))

        # add single subfield |0 to fields in marc record by identifier type
        if identifier_type in ['nlp_id', 'mms_id']:
            for flds_ids in terms_fields_ids.values():
                for field in flds_ids.get('raw_flds'):
                    if flds_ids.get('internal_ids'):
                        field.add_subfield('0', flds_ids.get('internal_ids').get(identifier_type))

        # add multiple subfields |0 to fields in marc record if identifier type == all_ids
        if identifier_type == 'all_ids':
            all_ids = {}

            if helper_list_for_ext_ids_query:
                external_ids = await conn_auth_ext.mget(*[nlp_id for term, nlp_id in helper_list_for_ext_ids_query])

                for term_nlp_id, ext_ids in zip(helper_list_for_ext_ids_query, external_ids):
                    if ext_ids:
                        fields_ids = terms_fields_ids.get(term_nlp_id[0])
                        fields_ids.setdefault('external_ids', json.loads(ext_ids))

            for flds_ids in terms_fields_ids.values():
                for field in flds_ids.get('raw_flds'):

                    i_ids = flds_ids.get('internal_ids')
                    if i_ids:
                        for id_type, ident in i_ids.items():
                            if ident and id_type != 'heading':
                                field.add_subfield('0', f'({id_type}){ident}')

                    e_ids = flds_ids.get('external_ids')
                    if e_ids:
                        for id_type, ident in e_ids.items():
                            if ident:
                                field.add_subfield('0', f'({id_type}){ident}')

    if polona:
        return terms_fields_ids
    else:
        return marc_record
