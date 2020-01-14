import re
import requests

from config.indexer_config import FIELDS_TO_CHECK


def prepare_name_for_indexing(value):
    if value:
        value = ''.join(char.replace('  ', ' ').replace(',', '').replace('.', '') for char in value)
        match = re.search(r'^\W+', value)
        if match:
            value = value[match.span(0)[1]:]
        match = re.search(r'\W+$', value)
        if match:
            value = value[:match.span(0)[0]]
    return value


def get_marc_bibliographic_data_from_data_bn(records_ids):
    records_ids_length = len(records_ids)

    if records_ids_length <= 100:
        ids_for_query = '%2C'.join(record_id for record_id in records_ids)
        query = 'http://data.bn.org.pl/api/bibs.marc?id={}&limit=100'.format(ids_for_query)

        result = bytearray(requests.get(query).content)
        return result


def normalize_nlp_id(nlp_id: str) -> str:
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


def process_record(marc_record, auth_index, identifier_type, auth_external_ids_index):
    """
    Main processing loop for adding authority identifiers to bibliographic record.
    """

    for marc_field_and_subfields in FIELDS_TO_CHECK:
        fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

        if fld in marc_record:
            raw_objects_flds_list = marc_record.get_fields(fld)

            for raw_fld in raw_objects_flds_list:
                term_to_search = prepare_name_for_indexing(' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                if term_to_search in auth_index:
                    single_identifier = None
                    all_ids = {}

                    if identifier_type == 'nlp_id':
                        single_identifier = list(auth_index.get(term_to_search).keys())[0]
                    if identifier_type == 'mms_id':
                        single_identifier = list(auth_index.get(term_to_search).values())[0]["mms_id"]
                    if identifier_type == 'all_ids':
                        nlp_id = list(auth_index.get(term_to_search).keys())[0]

                        all_ids.update({'nlp_id': nlp_id,
                                        'mms_id': list(auth_index.get(term_to_search).values())[0]["mms_id"],
                                        'viaf_uri': list(auth_index.get(term_to_search).values())[0]["viaf_id"],
                                        'coords': list(auth_index.get(term_to_search).values())[0]["coords"]})

                        ext_ids = auth_external_ids_index.get_ids(nlp_id)
                        if ext_ids:
                            all_ids.update(ext_ids)

                    if single_identifier:
                        marc_record.remove_field(raw_fld)
                        raw_fld.add_subfield('0', single_identifier)
                        marc_record.add_ordered_field(raw_fld)

                    if all_ids:
                        marc_record.remove_field(raw_fld)
                        for id_type, ident in all_ids.items():
                            if ident:
                                raw_fld.add_subfield('0', f'({id_type}){ident}')
                        marc_record.add_ordered_field(raw_fld)

    return marc_record
