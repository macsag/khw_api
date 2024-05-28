import io
import os
from typing import Optional

from dotenv import load_dotenv
from pymarc import marcxml
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from utils.marc_utils import process_record
from utils.pymarc_patches.xml_handler_patch import parse_xml_to_array_patched


load_dotenv('....')


DATA_BN_BIBS_BASE_ADDRESS = os.getenv("DATA_BN_BIBS_BASE_ADDRESS")

IS_LOCAL = os.getenv('IS_LOCAL')

LOC_HOST = os.getenv('LOC_HOST')
LOC_PORT = int(os.getenv('LOC_PORT'))

PROD_HOST = os.getenv('PROD_HOST')
PROD_PORT = int(os.getenv('PROD_PORT'))


class BibliographicRecordsHandler(object):
    def __init__(self,
                 aiohttp_client,
                 redis_client,
                 query,
                 identifier_type):
        self._aiohttp_client = aiohttp_client
        self._conn_auth_int = redis_client.auth_int
        self._conn_auth_ext = redis_client.auth_ext
        self._query = query
        self._identifier_type = identifier_type
        self._for_omnis = self._get_for_omnis_from_query()

    def _get_for_omnis_from_query(self) -> bool:
        for_omnis = self._query.get('for_omnis')
        if not for_omnis:
            return False
        if for_omnis:
            if for_omnis == 'false':
                return False
            elif for_omnis == 'true':
                return True
            else:
                return False

    async def _get_marcxml_response_from_data_bn_org_pl(self) -> tuple[int, Optional[bytes]]:
        if f'{DATA_BN_BIBS_BASE_ADDRESS}.marcxml?' not in self._query:
            processed_query = f'{DATA_BN_BIBS_BASE_ADDRESS}.marcxml?{self._query}'
        else:
            processed_query = self._query

        async with self._aiohttp_client.get(processed_query) as resp:
            resp_code = resp.status
            if resp_code == 200:
                marcxml_resp = await resp.read()
                return resp_code, marcxml_resp
            else:
                return resp_code, None

    @staticmethod
    def _get_next_page_for_data_bn_org_pl(marcxml_resp) -> str:
        root = ET.fromstring(marcxml_resp)
        if root[0].text:
            return root[0].text
        else:
            return ''

    def _create_next_page_for_user(self,
                                   next_page_for_data_bn_org_pl) -> str:
        if IS_LOCAL:
            base = f'{LOC_HOST}:{LOC_PORT}'
        else:
            base = PROD_HOST

        if next_page_for_data_bn_org_pl:
            query = next_page_for_data_bn_org_pl.split('marcxml?')[1]
            if self._for_omnis:
                next_page_for_user = escape(f'http://{base}/api/{self._identifier_type}/bibs?for_omnis=true&{query}')
            else:
                next_page_for_user = escape(f'http://{base}/api/{self._identifier_type}/bibs?{query}')
        else:
            next_page_for_user = ''

        return next_page_for_user

    @staticmethod
    def _read_marc_from_bytes_like_marcxml(marcxml_resp):
        return parse_xml_to_array_patched(io.BytesIO(marcxml_resp), normalize_form='NFC')

    async def _batch_process_records(self,
                                     marc_objects):
        enriched_recs = [await process_record(rcd,
                                              self._conn_auth_int,
                                              self._identifier_type,
                                              self._conn_auth_ext,
                                              for_omnis=self._for_omnis) for rcd in marc_objects]
        return enriched_recs

    @staticmethod
    def _produce_output_xml(next_page_for_user,
                            marc_enriched_objects):
        enriched_records_in_xml = []

        for rcd in marc_enriched_objects:
            xmlized_rcd = marcxml.record_to_xml(rcd, namespace=True)
            enriched_records_in_xml.append(bytearray(xmlized_rcd))

        joined = bytearray().join(enriched_records_in_xml)

        out_xml_beginning = f'<resp><nextPage>{next_page_for_user}</nextPage><collection>'.encode('utf-8')
        out_xml_end = '</collection></resp>'.encode('utf-8')
        out_xml = bytearray().join([out_xml_beginning, joined, out_xml_end])

        return bytes(out_xml)

    async def get_bibs(self):
        resp_code, marcxml_resp = await self._get_marcxml_response_from_data_bn_org_pl()

        if resp_code == 200:
            next_page_for_data_bn_org_pl = self._get_next_page_for_data_bn_org_pl(marcxml_resp)
            next_page_for_user = self._create_next_page_for_user(next_page_for_data_bn_org_pl)

            marc_objects = self._read_marc_from_bytes_like_marcxml(marcxml_resp)
            marc_enriched_objects = await self._batch_process_records(marc_objects)
            enriched_final_xml_output = self._produce_output_xml(next_page_for_user,
                                                                 marc_enriched_objects)

            return enriched_final_xml_output

        else:
            return resp_code
