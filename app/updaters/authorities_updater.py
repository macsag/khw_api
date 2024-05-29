import io
import logging
import os
import ujson
from datetime import datetime, UTC, timedelta
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from dotenv import load_dotenv

from utils.pymarc_patches.xml_handler_patch import parse_xml_to_array_patched

from utils.indexer_utils import (get_nlp_id, get_mms_id, get_viaf_id, get_coordinates, is_data_bn_ok,
                                 AuthorityRecordForIndexing, prepare_authority_record_for_indexing)
from utils import prepare_name_for_indexing
from utils.indexer_consts import AUTHORITY_INDEX_FIELDS
from utils.updater_utils import get_nlp_id_from_json


logger = logging.getLogger(__name__)

load_dotenv()


DATA_BN_AUTHORITIES_BASE_ADDRESS = os.getenv('DATA_BN_AUTHORITIES_BASE_ADDRESS')
TIMEDELTA = float(os.getenv('TIMEDELTA'))


class AuthorityUpdater(object):
    def __init__(self):
        self.update_in_progress = False
        self.last_authorities_update = ''

    async def update_authority_index(self,
                                     http_client,
                                     redis_client):
        # data.bn.org.pl health check
        if await is_data_bn_ok(http_client):

            # set updater status
            self.update_in_progress = True
            logger.info(f'Service status - data.bn.org.pl: OK.')
            logger.info(f'Switched authorities updater status to: {self.update_in_progress}.')

            # create dates for queries
            if not self.last_authorities_update:
                self.last_authorities_update = datetime.utcnow()

            date_from = self.last_authorities_update - timedelta(days=TIMEDELTA)
            date_from_iso_z = date_from.isoformat(timespec='seconds') + 'Z'

            date_to = datetime.utcnow()
            date_to_iso_z = date_to.isoformat(timespec='seconds') + 'Z'

            # set query address base
            query_addr_json = f'{DATA_BN_AUTHORITIES_BASE_ADDRESS}.json'
            query_addr_marcxml = f'{DATA_BN_AUTHORITIES_BASE_ADDRESS}.marcxml'

            # update authority records in authority index by record id (updates entries by record id and heading)
            logger.info(f'Started authorities update...')
            logger.info(f'Updating new/modified authotities...')
            updated_query = f'{query_addr_marcxml}?updatedDate={date_from_iso_z}%2C{date_to_iso_z}&limit=100'
            await self.update_updated_records_in_authority_index(updated_query, http_client, redis_client)
            logger.info(f'Done.')

            # get deleted authority records ids from data.bn.org.pl
            logger.info(f'Deleting removed authorities...')
            logger.info(f'Fetching authorities ids...')
            deleted_query = f'{query_addr_json}?updatedDate={date_from_iso_z}%2C{date_to_iso_z}&deleted=true&limit=100'
            deleted_records_ids = await self.get_records_ids_from_data_bn_for_authority_index_update(deleted_query,
                                                                                                     http_client)

            # delete authority records from authority index by record id (deletes entries by record id and heading)
            await self.remove_deleted_records_from_authority_index(deleted_records_ids, redis_client)
            logger.info(f'Deleted authorities: {len(deleted_records_ids)}.')

            # set updater status
            self.update_in_progress = False
            self.last_authorities_update = date_to
            logger.info(f'Authorities update succesfully completed.')
            logger.info(f'Switched authorities updater status to: {self.update_in_progress}.')

    @staticmethod
    async def yield_records_from_data_bn_for_authority_index_update(query, http_client):
        counter = 0
        while query:
            async with http_client.get(query) as resp:
                if resp.status == 200:
                    binary_content = await resp.read()
                    if binary_content:
                        xml_array = parse_xml_to_array_patched(io.BytesIO(binary_content), normalize_form='NFC')
                        root = ET.fromstring(binary_content.decode())
                        query = escape(root[0].text) if root[0].text else None
                        if not query:
                            logger.info(f'No (more) records to update.')
                        else:
                            counter += 1
                            logger.info(f'Processing query: {counter}.')
                        yield xml_array
                else:
                    logger.info(f'Service data.bn.org.pl unavailable. Aborting update...')
                    raise Exception

    async def update_updated_records_in_authority_index(
            self,
            updated_query,
            http_client,
            redis_client):
        async for rcd_array in self.yield_records_from_data_bn_for_authority_index_update(updated_query,
                                                                                          http_client):
            for rcd in rcd_array:
                if rcd:
                    for fld in AUTHORITY_INDEX_FIELDS:
                        if fld in rcd:
                            auth_rcd_to_index = prepare_authority_record_for_indexing(rcd, fld)
                            heading_to_index = prepare_name_for_indexing(auth_rcd_to_index.heading)

                            if auth_rcd_to_index.nlp_id:
                                # check if record was already indexed and if so, get the old version
                                old_auth_rcd_from_redis = await redis_client.auth_int.get(auth_rcd_to_index.nlp_id)

                                if not old_auth_rcd_from_redis:
                                    # record is completely new and it has to be indexed
                                    auth_rcd_to_index_as_json = auth_rcd_to_index.as_json()

                                    await redis_client.auth_int.mset(
                                        {auth_rcd_to_index.nlp_id: auth_rcd_to_index_as_json,
                                         heading_to_index: auth_rcd_to_index_as_json})

                                    logging.debug(f'Indexed new record: {auth_rcd_to_index.nlp_id}/{heading_to_index}.')

                                    break  # breaks only the inner loop (searching for fields to index)

                                else:
                                    # rcd was already indexed and there is the old version
                                    old_auth_rcd_from_redis = AuthorityRecordForIndexing._make(
                                        ujson.loads(old_auth_rcd_from_redis).values())

                                    # compare headings
                                    if old_auth_rcd_from_redis.heading == heading_to_index:
                                        # heading wasn't modified
                                        if old_auth_rcd_from_redis == auth_rcd_to_index:
                                            # nothing's changed, do nothing
                                            break  # breaks only the inner loop (searching for fields to index)

                                        else:
                                            # something's changed
                                            if old_auth_rcd_from_redis.heading_tag != '130' and fld == '130':
                                                # if the new one (currently processed) has 130 heading tag
                                                # and the old one has different heading tag
                                                # just skip it (break the loop)

                                                break  # breaks only the inner loop (searching for fields to index)
                                            else:
                                                # if the new one (currently processed) has not 130 heading
                                                # do the usual job
                                                # record will be normally processed
                                                # and will overwrite the existing record

                                                auth_rcd_to_index_as_json = auth_rcd_to_index.as_json()
                                                await redis_client.auth_int.mset(
                                                    {auth_rcd_to_index.nlp_id: auth_rcd_to_index_as_json,
                                                     heading_to_index: auth_rcd_to_index_as_json})

                                                break  # breaks only the inner loop (searching for fields to index)
                                    else:
                                        # heading was modified
                                        await redis_client.auth_int.delete(old_auth_rcd_from_redis.heading)
                                        auth_rcd_to_index_as_json = auth_rcd_to_index.as_json()
                                        await redis_client.auth_int.mset(
                                            {auth_rcd_to_index.nlp_id: auth_rcd_to_index_as_json,
                                             heading_to_index: auth_rcd_to_index_as_json})

                                        break  # breaks only the inner loop (searching for fields to index)

    @staticmethod
    async def get_records_ids_from_data_bn_for_authority_index_update(query, http_client):
        records_ids = []

        while query:
            async with http_client.get(query) as resp:
                if resp.status == 200:
                    json_chunk = await resp.json()

                    for rcd in json_chunk['authorities']:
                        record_id = get_nlp_id_from_json(rcd)
                        records_ids.append(record_id)

                    query = json_chunk['nextPage'] if json_chunk['nextPage'] else None
                else:
                    break

        return records_ids

    @staticmethod
    async def remove_deleted_records_from_authority_index(records_ids, redis_client):
        for record_id in records_ids:
            auth_to_delete = await redis_client.auth_int.get(record_id)
            if auth_to_delete:
                heading = prepare_name_for_indexing(ujson.loads(auth_to_delete).get('heading'))
                await redis_client.auth_int.delete(heading)
                await redis_client.auth_int.delete(record_id)






