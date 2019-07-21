import requests
import logging
from pymarc import marcxml, MARCReader

from utils.marc_utils import process_record
from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST


class BibliographicRecordsChunk(object):
    def __init__(self, query, auth_index, bib_index, identifier_type):
        self.query = query
        self.identifier_type = identifier_type
        self.json_response = self.get_json_response()

        self.next_page_for_data_bn = self.get_next_page_for_data_bn()
        self.next_page_for_user = self.create_next_page_for_user()
        self.records_ids = self.get_bibliographic_records_ids_from_data_bn()
        self.marc_chunk = self.get_bibliographic_records_in_marc_from_local_bib_index(bib_index)
        self.marc_objects_chunk = self.read_marc_from_binary_in_chunks()
        self.marc_processed_objects_chunk = self.batch_process_records(auth_index)
        self.xml_processed_chunk = self.produce_output_xml()

    def get_json_response(self):
        logging.debug(self.query)
        if 'http://data.bn.org.pl/api/bibs.json?{}' not in self.query:
            processed_query = 'http://data.bn.org.pl/api/bibs.json?{}'.format(self.query)
            logging.debug(processed_query)
        else:
            processed_query = self.query
            logging.debug(processed_query)

        r = requests.get(processed_query)
        json_chunk = r.json()
        return json_chunk

    def get_bibliographic_records_ids_from_data_bn(self):
        records_ids = []

        for rcd in self.json_response['bibs']:
            record_id = rcd['marc']['fields'][0]['001']
            records_ids.append(record_id)

        logging.debug(records_ids)

        return records_ids

    def get_next_page_for_data_bn(self):
        return self.json_response['nextPage']

    def create_next_page_for_user(self):
        if IS_LOCAL:
            base = f'{LOC_HOST}:{LOC_PORT}'
        else:
            base = PROD_HOST
        if self.next_page_for_data_bn:
            query = self.next_page_for_data_bn.split('json?')[1]

            next_page_for_user = f'http://{base}/api/{self.identifier_type}/bibs?{query}'
        else:
            next_page_for_user = ''

        return next_page_for_user

    def get_bibliographic_records_in_marc_from_local_bib_index(self, bib_index):
        marc_data_chunk_list = []

        for record_id in self.records_ids:
            if record_id in bib_index:
                marc_data_chunk_list.append(bib_index[record_id])

        logging.debug(marc_data_chunk_list)

        marc_data_chunk_joined_to_one_bytearray = bytearray().join(marc_data_chunk_list)

        logging.debug(marc_data_chunk_joined_to_one_bytearray)

        return marc_data_chunk_joined_to_one_bytearray

    def read_marc_from_binary_in_chunks(self):
        marc_objects_chunk = []

        marc_rdr = MARCReader(self.marc_chunk, to_unicode=True, force_utf8=True, utf8_handling='ignore')
        for rcd in marc_rdr:
            marc_objects_chunk.append(rcd)

        logging.debug(marc_objects_chunk)

        return marc_objects_chunk

    def batch_process_records(self, auth_index):
        logging.debug(self.identifier_type)
        processed_records = [process_record(rcd, auth_index, self.identifier_type) for rcd in self.marc_objects_chunk]

        logging.debug(processed_records)

        return processed_records

    def produce_output_xml(self):
        wrapped_processed_records_in_xml = []

        for rcd in self.marc_processed_objects_chunk:
            print(rcd)
            xmlized_rcd = marcxml.record_to_xml(rcd, namespace=True)
            wrapped_rcd = '<bib>' + str(xmlized_rcd)[2:-1] + '</bib>'
            wrapped_processed_records_in_xml.append(wrapped_rcd)

        joined_to_str = ''.join(rcd for rcd in wrapped_processed_records_in_xml)

        out_xml = '<resp><nextPage>{}</nextPage><bibs>{}</bibs></resp>'.format(self.next_page_for_user, joined_to_str)

        return out_xml
