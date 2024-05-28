import io
import logging
import ujson

from datetime import datetime, UTC, timedelta
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from app.utils.pymarc_patches.xml_handler_patch import parse_xml_to_array_patched

from app.utils import get_nlp_id, get_mms_id, get_viaf_id, get_coordinates, is_data_bn_ok
from app.utils import prepare_name_for_indexing
from app.utils.updater_utils import get_nlp_id_from_json

from app.config.indexer_config import AUTHORITY_INDEX_FIELDS
from app.config.timedelta_config import TIMEDELTA_CONFIG


logger = logging.getLogger(__name__)


class AuthorityUpdater(object):
    def __init__(self):
        self.update_in_progress = False
        self.last_authorities_update = datetime.now(UTC)

    async def update_authority_index(self,
                                     http_client,
                                     redis_client):
        # data.bn.org.pl health check
        if await is_data_bn_ok(http_client):

            # set updater status
            self.update_in_progress = True
            logger.info(f'Status data.bn.org.pl: OK.')
            logger.info(f'Zmieniono status updatera rekordów wzorcowych na: {self.update_in_progress}.')

            # create dates for queries
            date_from = self.last_authorities_update - timedelta(days=TIMEDELTA_CONFIG)
            date_from_iso_z = date_from.isoformat(timespec='seconds') + 'Z'

            date_to = datetime.utcnow()
            date_to_iso_z = date_to.isoformat(timespec='seconds') + 'Z'

            # set query address base
            query_addr_json = 'http://data.bn.org.pl/api/authorities.json'
            query_addr_marcxml = 'http://data.bn.org.pl/api/authorities.marcxml'

            # update authority records in authority index by record id (updates entries by record id and heading)
            logger.info(f'Rozpoczynam aktualizację rekordów wzorcowych.')
            logger.info(f'Rozpoczynam aktualizację rekordów wzorcowych nowych/zaktualizowanych.')
            updated_query = f'{query_addr_marcxml}?updatedDate={date_from_iso_z}%2C{date_to_iso_z}&limit=100'
            await self.update_updated_records_in_authority_index(updated_query, http_client, redis_client)
            logger.info(f'Zaktualizowano.')

            # get deleted authority records ids from data.bn.org.pl
            logger.info(f'Rozpoczynam usuwanie rekordów wzorcowych usuniętych.')
            logger.info(f'Pobieram identyfikatory.')
            deleted_query = f'{query_addr_json}?updatedDate={date_from_iso_z}%2C{date_to_iso_z}&deleted=true&limit=100'
            deleted_records_ids = await self.get_records_ids_from_data_bn_for_authority_index_update(deleted_query,
                                                                                                     http_client)

            # delete authority records from authority index by record id (deletes entries by record id and heading)
            await self.remove_deleted_records_from_authority_index(deleted_records_ids, redis_client)
            logger.info("Usunięto rekordów: {}".format(len(deleted_records_ids)))

            # set updater status
            self.update_in_progress = False
            self.last_authorities_update = date_to
            logger.info(f'Zakończono aktualizację rekordów wzorcowych.')
            logger.info(f'Zmieniono status updatera rekordów wzorcowych na: {self.update_in_progress}.')

    @staticmethod
    async def yield_records_from_data_bn_for_authority_index_update(query, aiohttp_session):
        counter = 0
        while query:
            async with aiohttp_session.get(query) as resp:
                if resp.status == 200:
                    binary_content = await resp.read()
                    if binary_content:
                        xml_array = parse_xml_to_array_patched(io.BytesIO(binary_content), normalize_form='NFC')
                        root = ET.fromstring(binary_content.decode())
                        query = escape(root[0].text) if root[0].text else None
                        if not query:
                            logger.info(f'Brak rekordów do przetworzenia lub koniec przetwarzania.')
                        else:
                            counter += 1
                            logger.info(f'Przekazano do przetworzenia paczkę nr {counter}.')
                        yield xml_array
                else:
                    logger.info(f'Pojawił się problem z data.bn.org.pl. Przerywam przetwarzanie.')
                    break

    async def update_updated_records_in_authority_index(self, updated_query, aiohttp_session, conn_auth_int):
        async for rcd_array in self.yield_records_from_data_bn_for_authority_index_update(updated_query,
                                                                                          aiohttp_session):
            for rcd in rcd_array:
                if rcd:
                    for fld in AUTHORITY_INDEX_FIELDS:
                        if fld in rcd:
                            heading_full = rcd.get_fields(fld)[0].value()
                            heading_to_index = prepare_name_for_indexing(heading_full)
                            mms_id = get_mms_id(rcd)
                            nlp_id = get_nlp_id(rcd)
                            viaf_id = get_viaf_id(rcd)
                            coordinates = get_coordinates(rcd)

                            to_update = {'nlp_id': nlp_id,
                                         'mms_id': mms_id,
                                         'viaf_id': viaf_id,
                                         'coords': coordinates,
                                         'heading': heading_full,
                                         'heading_tag': fld}

                            json_to_update = json.dumps(to_update, ensure_ascii=False)

                            if nlp_id:
                                # check if record was already indexed and if so, get the old version
                                auth_to_update = await conn_auth_int.get(nlp_id)

                                if auth_to_update:
                                    # rcd was already indexed and there is old version
                                    auth_to_update_dict = json.loads(auth_to_update)

                                    # get the old heading for comparison
                                    old_heading = prepare_name_for_indexing(auth_to_update_dict.get('heading'))

                                    if old_heading == heading_to_index:
                                        # heading wasn't modified
                                        if to_update == auth_to_update_dict:
                                            # nothing's changed, do nothing
                                            break  # breaks only the inner loop (searching for fields to index)

                                        else:
                                            # something's changed
                                            if auth_to_update_dict.get('heading_tag') != '130' and fld == '130':
                                                # if authority with the same heading is already indexed
                                                # and the new one (currently processed) is 130 heading
                                                # just skip it (break the loop)

                                                break  # breaks only the inner loop (searching for fields to index)
                                            else:
                                                # if authority with the same heading is already indexed
                                                # and the new one (currently processed) is not 130 heading
                                                # do the usual job
                                                # record will be normally processed
                                                # and will overwrite the existing record

                                                await conn_auth_int.mset({nlp_id: json_to_update,
                                                                         heading_to_index: json_to_update})

                                                break  # breaks only the inner loop (searching for fields to index)
                                    else:
                                        # heading was modified
                                        await conn_auth_int.delete(old_heading)
                                        await conn_auth_int.mset({nlp_id: json_to_update,
                                                                 heading_to_index: json_to_update})

                                        break  # breaks only the inner loop (searching for fields to index)

                                else:
                                    # record is new and it has to be indexed

                                    await conn_auth_int.mset({nlp_id: json_to_update,
                                                              heading_to_index: json_to_update})

                                    logging.debug(f'Dodano nowe hasło: {heading_to_index}')

                                    break  # breaks only the inner loop (searching for fields to index)

    @staticmethod
    async def get_records_ids_from_data_bn_for_authority_index_update(query, aiohttp_session):
        records_ids = []

        while query:
            async with aiohttp_session.get(query) as resp:
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
    async def remove_deleted_records_from_authority_index(records_ids, conn_auth_int):
        for record_id in records_ids:
            auth_to_delete = await conn_auth_int.get(record_id)
            if auth_to_delete:
                heading = prepare_name_for_indexing(json.loads(auth_to_delete).get('heading'))
                await conn_auth_int.delete(heading)
                await conn_auth_int.delete(record_id)






