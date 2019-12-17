import requests
import io

import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from pymarc import marcxml
from pymarc_patches.xml_handler_patch import parse_xml_to_array_patched

from utils.marc_utils import process_record
from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST


class BibliographicRecordsChunk(object):
    def __init__(self, query, auth_index, identifier_type, auth_external_ids_index):
        self.query = query
        self.identifier_type = identifier_type
        self.response_code = None
        self.marcxml_response_object = self.get_marcxml_response()
        self.next_page_for_data_bn = None
        self.next_page_for_user = None
        self.marc_objects_chunk = None
        self.marc_processed_objects_chunk = None
        self.xml_processed_chunk = None
        self.process_response(auth_index, auth_external_ids_index)

    def get_marcxml_response(self):
        if 'http://data.bn.org.pl/api/bibs.marcxml?{}' not in self.query:
            processed_query = f'http://data.bn.org.pl/api/bibs.marcxml?{self.query}'
        else:
            processed_query = self.query

        r = requests.get(processed_query)
        self.response_code = r.status_code
        return r

    def process_response(self, auth_index, auth_external_ids_index):
        if self.response_code == 200:
            self.next_page_for_data_bn = self.get_next_page_for_data_bn()
            self.next_page_for_user = self.create_next_page_for_user()
            self.marc_objects_chunk = self.read_marc_from_bytes_like_marcxml()
            self.marc_processed_objects_chunk = self.batch_process_records(auth_index, auth_external_ids_index)
            self.xml_processed_chunk = self.produce_output_xml()

    def get_next_page_for_data_bn(self):
        root = ET.fromstring(self.marcxml_response_object.content)
        if root[0].text:
            return root[0].text
        else:
            return ''

    def create_next_page_for_user(self):
        if IS_LOCAL:
            base = f'{LOC_HOST}:{LOC_PORT}'
        else:
            base = PROD_HOST
        if self.next_page_for_data_bn:
            query = self.next_page_for_data_bn.split('marcxml?')[1]

            next_page_for_user = escape(f'http://{base}/api/{self.identifier_type}/bibs?{query}')
        else:
            next_page_for_user = ''

        return next_page_for_user

    def read_marc_from_bytes_like_marcxml(self):
        if self.marcxml_response_object.content:
            return parse_xml_to_array_patched(io.BytesIO(self.marcxml_response_object.content), normalize_form='NFC')
        else:
            return None

    def batch_process_records(self, auth_index, auth_external_ids_index):
        if self.marc_objects_chunk:
            processed_recs = [process_record(rcd,
                                             auth_index,
                                             self.identifier_type,
                                             auth_external_ids_index) for rcd in self.marc_objects_chunk]
            return processed_recs
        else:
            return None

    def produce_output_xml(self):
        if self.marc_processed_objects_chunk:
            processed_records_in_xml = []

            for rcd in self.marc_processed_objects_chunk:
                xmlized_rcd = marcxml.record_to_xml(rcd, namespace=True)
                processed_records_in_xml.append(bytearray(xmlized_rcd))

            joined = bytearray().join(processed_records_in_xml)

            out_xml_beginning = f'<resp><nextPage>{self.next_page_for_user}</nextPage><collection>'.encode('utf-8')
            out_xml_end = '</collection></resp>'.encode('utf-8')
            out_xml = bytearray().join([out_xml_beginning, joined, out_xml_end])

            return bytes(out_xml)
        else:
            return f'<resp><nextPage/><collection> </collection></resp>'.encode('utf-8')
