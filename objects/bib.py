from typing import Optional
import io
from asyncinit import asyncinit

import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from pymarc import marcxml
from pymarc_patches.xml_handler_patch import parse_xml_to_array_patched

from utils.marc_utils import process_record
from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST


@asyncinit
class BibliographicRecordsChunk(object):
    async def __init__(self, aiohttp_session, conn_auth_int, conn_auth_ext, query, identifier_type):
        self.aiohttp_session = aiohttp_session
        self.conn_auth_int = conn_auth_int
        self.conn_auth_ext = conn_auth_ext
        self.query = query
        self.identifier_type = identifier_type
        self.response_code = None
        self.marcxml_response_content = await self.get_marcxml_response()
        self.next_page_for_data_bn = None
        self.next_page_for_user = None
        self.marc_objects_chunk = None
        self.marc_processed_objects_chunk = None
        self.xml_processed_chunk = await self.process_response()


    async def get_marcxml_response(self) -> Optional[bytes]:
        if 'http://data.bn.org.pl/api/bibs.marcxml?{}' not in self.query:
            processed_query = f'http://data.bn.org.pl/api/bibs.marcxml?{self.query}'
        else:
            processed_query = self.query

        async with self.aiohttp_session.get(processed_query) as response:
            self.response_code = response.status
            if self.response_code == 200:
                return await response.read()
            else:
                return None

    async def process_response(self):
        if self.response_code == 200:
            self.next_page_for_data_bn = self.get_next_page_for_data_bn()
            self.next_page_for_user = self.create_next_page_for_user()
            self.marc_objects_chunk = await self.read_marc_from_bytes_like_marcxml()
            self.marc_processed_objects_chunk = await self.batch_process_records()
            xml_processed_chunk = self.produce_output_xml()
            return xml_processed_chunk

    def get_next_page_for_data_bn(self):
        root = ET.fromstring(self.marcxml_response_content)
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

    async def read_marc_from_bytes_like_marcxml(self):
        return parse_xml_to_array_patched(io.BytesIO(self.marcxml_response_content), normalize_form='NFC')

    async def batch_process_records(self):
            processed_recs = [await process_record(rcd,
                                                   self.conn_auth_int,
                                                   self.identifier_type,
                                                   self.conn_auth_ext) for rcd in self.marc_objects_chunk]
            return processed_recs

    def produce_output_xml(self):
        processed_records_in_xml = []

        for rcd in self.marc_processed_objects_chunk:
            xmlized_rcd = marcxml.record_to_xml(rcd, namespace=True)
            processed_records_in_xml.append(bytearray(xmlized_rcd))

        joined = bytearray().join(processed_records_in_xml)

        out_xml_beginning = f'<resp><nextPage>{self.next_page_for_user}</nextPage><collection>'.encode('utf-8')
        out_xml_end = '</collection></resp>'.encode('utf-8')
        out_xml = bytearray().join([out_xml_beginning, joined, out_xml_end])

        return bytes(out_xml)
