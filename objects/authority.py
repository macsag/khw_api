import io
from typing import Optional

import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from pymarc import marcxml
from pymarc_patches.xml_handler_patch import parse_xml_to_array_patched

from asyncinit import asyncinit

from utils.marc_utils import process_record
from config.base_url_config import IS_LOCAL, LOC_HOST, LOC_PORT, PROD_HOST

@asyncinit
class AuthorityRecordsChunk(object):
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
        if 'http://data.bn.org.pl/api/authorities.marcxml?{}' not in self.query:
            processed_query = f'http://data.bn.org.pl/api/authorities.marcxml?{self.query}'
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



class OldAuthorityRecordsChunk(object):
    def __init__(self, authority_ids, local_auth_index, local_auth_external_ids_index):
        self.authority_ids = authority_ids
        self.json_processed_chunk = self.get_all_ids(local_auth_index, local_auth_external_ids_index)

    def get_all_ids(self, local_auth_index, local_auth_external_ids_index) -> dict:
        dict_to_return = {}

        try:
            authority_ids_list = self.authority_ids.split(',')
        except ValueError:
            authority_ids_list = [self.authority_ids]

        for authority_id in authority_ids_list:
            internal_ids_and_viaf = self.get_internal_ids_and_viaf(local_auth_index, authority_id)
            external_ids = self.get_external_ids(local_auth_external_ids_index, authority_id)
            merged_dict = self.merge_results(authority_id, internal_ids_and_viaf, external_ids)

            if merged_dict:
                dict_to_return.update(merged_dict)
            else:
                dict_to_return.setdefault(authority_id, None)

        return dict_to_return

    @staticmethod
    def get_internal_ids_and_viaf(local_auth_index: dict, authority_id: str) -> Optional[dict]:
        name = local_auth_index.get(authority_id)
        ids = local_auth_index.get(name) if name else None
        ids_to_return = ids.get(authority_id) if ids else None
        return ids_to_return if ids_to_return else None

    @staticmethod
    def get_external_ids(local_auth_external_ids_index, authority_id: str) -> Optional[dict]:
        ids = local_auth_external_ids_index.get_ids(authority_id)
        return ids if ids else None

    @staticmethod
    def merge_results(authority_id: str, internal_ids_and_viaf: dict, external_ids) -> Optional[dict]:
        result_dict = {}

        if internal_ids_and_viaf:
            result_dict.setdefault(authority_id, {}).update(internal_ids_and_viaf)
        if external_ids:
            result_dict.setdefault(authority_id, {}).update(external_ids)

        return result_dict if result_dict else None
