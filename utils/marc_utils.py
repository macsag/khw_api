import requests
from pymarc import MARCReader
from pymarc import Field

from config.indexer_config import FIELDS_TO_CHECK


def get_rid_of_punctuation(string):
    return ''.join(char.replace(',', '').replace('.', '') for char in string)


def read_marc_from_binary(data_chunk):
    marc_rdr = MARCReader(data_chunk, to_unicode=True, force_utf8=True, utf8_handling='ignore')
    for rcd in marc_rdr:
        return rcd


def calculate_check_digit(record_id):
    char_sum = 0
    i = 2
    for character in record_id[::-1]:
        char_sum += int(character) * i
        i += 1
    remainder = char_sum % 11
    check_digit = str(remainder) if remainder != 10 else 'x'
    return record_id + check_digit


def get_marc_authority_data_from_data_bn(records_ids):
    records_ids_length = len(records_ids)

    if records_ids_length <= 100:
        ids_for_query = '%2C'.join(record_id for record_id in records_ids)
        query = 'http://data.bn.org.pl/api/authorities.marc?id={}&limit=100'.format(ids_for_query)

        result = bytearray(requests.get(query).content)
        return result


def get_single_marc_authority_record_from_data_bn(record_id):
    query = 'http://data.bn.org.pl/api/authorities.marc?id={}'.format(record_id)
    return bytearray(requests.get(query).content)


def get_marc_bibliographic_data_from_data_bn(records_ids):
    records_ids_length = len(records_ids)

    if records_ids_length <= 100:
        ids_for_query = '%2C'.join(record_id for record_id in records_ids)
        query = 'http://data.bn.org.pl/api/bibs.marc?id={}&limit=100'.format(ids_for_query)

        result = bytearray(requests.get(query).content)
        return result


def process_record(marc_record, auth_index, identifier_type):
    """
    Main processing loop for adding authority identifiers to bibliographic record.
    """

    for marc_field_and_subfields in FIELDS_TO_CHECK:
        fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

        if fld in marc_record:
            raw_objects_flds_list = marc_record.get_fields(fld)

            for raw_fld in raw_objects_flds_list:
                term_to_search = get_rid_of_punctuation(' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                if term_to_search in auth_index:
                    identifier = None

                    if identifier_type == 'nlp_id':
                        identifier = list(auth_index.get(term_to_search).keys())[0]
                    if identifier_type == 'mms_id':
                        identifier = list(auth_index.get(term_to_search).values())[0]

                    if identifier:
                        marc_record.remove_field(raw_fld)
                        raw_fld.add_subfield('0', identifier)
                        marc_record.add_ordered_field(raw_fld)

    return marc_record
