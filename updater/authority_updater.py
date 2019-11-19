import requests
from pymarc import parse_xml_to_array
from datetime import datetime, timedelta
import logging
import io

import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from utils.indexer_utils import get_nlp_id, get_mms_id, get_viaf_id, is_data_bn_ok
from utils.marc_utils import prepare_name_for_indexing
from utils.updater_utils import get_nlp_id_from_json

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from config.timedelta_config import TIMEDELTA_CONFIG


class AuthorityUpdater(object):
    def __init__(self):
        pass

    def update_authority_index(self, authority_index, updater_status):
        # data.bn.org.pl health check
        if is_data_bn_ok():

            # set updater status
            updater_status.update_in_progress = True
            logging.info(f'AUTHORITIES | Zmieniono status updatera na: {updater_status.update_in_progress}.')

            # create dates for queries
            date_from = updater_status.last_auth_update - timedelta(days=TIMEDELTA_CONFIG)
            date_from_iso_z = date_from.isoformat(timespec='seconds') + 'Z'

            date_to = datetime.utcnow()
            date_to_iso_z = date_to.isoformat(timespec='seconds') + 'Z'

            # set query address base
            query_addr_json = 'http://data.bn.org.pl/api/authorities.json'
            query_addr_marcxml = 'http://data.bn.org.pl/api/authorities.marcxml'

            # update authority records in authority index by record id (updates entries by record id and heading)
            updated_query = f'{query_addr_marcxml}?updatedDate={date_from_iso_z}%2C{date_to_iso_z}&limit=100'
            self.update_updated_records_in_authority_index(updated_query, authority_index)

            # get deleted authority records ids from data.bn.org.pl
            deleted_query = f'{query_addr_json}?updatedDate={date_from_iso_z}%2C{date_to_iso_z}&deleted=true&limit=100'
            deleted_records_ids = self.get_records_ids_from_data_bn_for_authority_index_update(deleted_query)
            logging.info("Rekordów wzorcowych usuniętych: {}".format(len(deleted_records_ids)))

            # delete authority records from authority index by record id (deletes entries by record id and heading)
            self.remove_deleted_records_from_authority_index(deleted_records_ids, authority_index)

            # set updater status
            updater_status.update_in_progress = False
            updater_status.last_auth_update = date_to
            logging.info(f'AUTHORITIES | Zmieniono status updatera na: {updater_status.update_in_progress}')

    @staticmethod
    def yield_records_from_data_bn_for_authority_index_update(query):
        while query:
            r = requests.get(query)
            if r.status_code == 200:
                if r.content:
                    xml_array = parse_xml_to_array(io.BytesIO(r.content))
                    root = ET.fromstring(r.content)
                    query = escape(root[0].text) if root[0].text else None
                    yield xml_array
            else:
                break

    def update_updated_records_in_authority_index(self, updated_query, authority_index):
        for rcd_array in self.yield_records_from_data_bn_for_authority_index_update(updated_query):
            for rcd in rcd_array:
                if rcd:
                    for fld in AUTHORITY_INDEX_FIELDS:
                        if fld in rcd:
                            heading = prepare_name_for_indexing(rcd.get_fields(fld)[0].value())
                            mms_id = get_mms_id(rcd)
                            nlp_id = get_nlp_id(rcd)
                            viaf_id = get_viaf_id(rcd)

                            if nlp_id:

                                if nlp_id in authority_index:  # rcd is old and already indexed
                                    old_heading = authority_index.get(nlp_id)
                                    if old_heading == heading:  # heading wasn't modified - break and continue
                                        break
                                    else:  # heading was modified
                                        authority_index[nlp_id] = heading  # set new heading for this id

                                        old_heading_ids = authority_index[old_heading]  # get ref to old heading ids

                                        if len(old_heading_ids) > 1:  # there is more than one id for the heading
                                            old_heading_ids.pop(nlp_id, None)  # delete the obsolete id
                                            logging.debug(f'Usunięto zestaw id z (mod): {old_heading}')

                                            # set new ids
                                            authority_index.setdefault(heading, {}).update({nlp_id: {'mms_id': mms_id, 'viaf_id': viaf_id}})
                                            break
                                        else:  # there is only one dict of ids
                                            authority_index.pop(old_heading, None)  # delete entry completely
                                            logging.debug(f'Usunięto hasło całkowicie (mod): {old_heading}')

                                            # set new ids
                                            authority_index.setdefault(heading, {}).update({nlp_id: {'mms_id': mms_id, 'viaf_id': viaf_id}})
                                            break
                                else:  # rcd is new and it has to be indexed
                                    authority_index[nlp_id] = heading
                                    authority_index.setdefault(heading, {}).update({nlp_id: {'mms_id': mms_id, 'viaf_id': viaf_id}})
                                    logging.debug(f'Dodano nowe hasło (new): {heading}')
                                    break

    @staticmethod
    def get_records_ids_from_data_bn_for_authority_index_update(query):
        records_ids = []

        while query:
            r = requests.get(query)
            if r.status_code == 200:
                json_chunk = r.json()

                for rcd in json_chunk['authorities']:
                    record_id = get_nlp_id_from_json(rcd)
                    records_ids.append(record_id)

                query = json_chunk['nextPage'] if json_chunk['nextPage'] else None
            else:
                break

        return records_ids

    @staticmethod
    def remove_deleted_records_from_authority_index(records_ids, authority_index):
        for record_id in records_ids:
            if record_id in authority_index:
                heading = authority_index.pop(record_id)
                heading_ids = authority_index[heading]
                if len(heading_ids) > 1:
                    heading_ids.pop(record_id, None)
                    logging.debug(f'Usunięto zestaw id z (del): {heading}')
                else:
                    authority_index.pop(heading, None)
                    logging.debug(f'Usunięto hasło całkowicie (del): {heading}')
