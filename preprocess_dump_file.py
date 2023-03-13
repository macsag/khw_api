from typing import List
import ujson

from pymarc import MARCReader, Record
import redis
from tqdm import tqdm

from config.indexer_config import FIELDS_TO_CHECK_FOR_OMNIS
from utils.marc_utils import prepare_name_for_indexing


CONN_INT = redis.Redis(db=8)


def get_terms_to_search_and_references_to_raw_flds(marc_record) -> dict:
    terms_fields_ids = {}

    for marc_field_and_subfields in FIELDS_TO_CHECK_FOR_OMNIS:
        fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

        if fld in marc_record:
            raw_objects_flds_list = marc_record.get_fields(fld)

            for raw_fld in raw_objects_flds_list:
                term_to_search = prepare_name_for_indexing(
                    ' '.join(subfld for subfld in raw_fld.get_subfields(*subflds)))

                terms_fields_ids.setdefault(term_to_search, {}).setdefault('raw_flds', []).append(raw_fld)

    return terms_fields_ids


def process_record(marc_record, conn_auth_int, identifier_type):
    """
    Main processing loop for adding authority identifiers to bibliographic record.
    """

    # get all terms to search for in redis index and get all references to raw flds with these terms
    terms_fields_ids = get_terms_to_search_and_references_to_raw_flds(marc_record)

    # get all internal ids (meaning: from original NLP database via data.bn.org.pl) from redis index db=0
    internal_ids = conn_auth_int.mget(*list(terms_fields_ids.keys()))

    for term, int_ids in zip(list(terms_fields_ids.keys()), internal_ids):
        if int_ids:
            fields_ids = terms_fields_ids.get(term)
            in_json = ujson.loads(int_ids)
            fields_ids.setdefault('internal_ids', in_json)

    # add single subfield |0 to fields in marc record by identifier type
    if identifier_type in ['nlp_id', 'mms_id']:
        for flds_ids in terms_fields_ids.values():
            for field in flds_ids.get('raw_flds'):
                if flds_ids.get('internal_ids'):
                    field.add_subfield('0', flds_ids.get('internal_ids').get(identifier_type))

    return marc_record


def yield_record_from_file(input_file: str) -> Record:
    with open(input_file, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)
        for rcd in rdr:
            yield rcd


def write_to_file(output_file: str, records_to_write: List[Record]) -> None:
    with open(output_file, 'ab') as fp:
        for rcd in records_to_write:
            fp.write(rcd.as_marc())


def main_loop(input_file, output_file):
    write_buffer = []

    for rcd in tqdm(yield_record_from_file(input_file)):
        processed_rcd = process_record(rcd, CONN_INT, 'nlp_id')
        write_buffer.append(processed_rcd)

        if len(write_buffer) > 1000:
            write_to_file(output_file, write_buffer)
            write_buffer.clear()

    write_to_file(output_file, write_buffer)


if __name__ == "__main__":

    file_in = 'bibs-artykul.mrc'
    file_out = 'bibs_artykul_pre_2023_02_07.mrc'

    main_loop(file_in, file_out)
