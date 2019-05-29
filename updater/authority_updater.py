import requests
from pymarc import MARCReader
from datetime import datetime, timedelta
import logging

from utils.indexer_utils import get_nlp_id, prepare_dict_of_authority_ids_to_append, is_data_bn_ok
from utils.marc_utils import get_rid_of_punctuation, get_marc_authority_data_from_data_bn
from utils.updater_utils import get_nlp_id_from_json

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from config.timedelta_config import TIMEDELTA_CONFIG


class AuthorityUpdater(object):
    def __init__(self):
        pass

    def update_authority_index(self, authority_index, updater_status):
        # health check
        if is_data_bn_ok():
            # set update status
            updater_status.update_in_progress = True
            logging.info("Zmieniono status updatera na: {}".format(updater_status.update_in_progress))

            # create dates for queries
            date_from = updater_status.last_auth_update - timedelta(days=TIMEDELTA_CONFIG)
            date_from_in_iso_with_z = date_from.isoformat(timespec='seconds') + 'Z'
            date_to = datetime.utcnow()
            date_to_in_iso_with_z = date_to.isoformat(timespec='seconds') + 'Z'

            # get deleted authority records ids from data.bn.org.pl
            deleted_query = 'http://data.bn.org.pl/api/authorities.json?updatedDate={}%2C{}&deleted=true&limit=100'.format(
                                                                            date_from_in_iso_with_z, date_to_in_iso_with_z)

            deleted_records_ids = self.get_records_ids_from_data_bn_for_authority_index_update(deleted_query)
            logging.info("Rekordów wzorcowych usuniętych: {}".format(len(deleted_records_ids)))

            # get updated authority records ids from data.bn.org.pl
            updated_query = 'http://data.bn.org.pl/api/authorities.json?updatedDate={}%2C{}&limit=100'.format(
                                                                date_from_in_iso_with_z, date_to_in_iso_with_z)

            updated_records_ids = self.get_records_ids_from_data_bn_for_authority_index_update(updated_query)
            logging.info("Rekordów wzorcowych zmodyfikowanych: {}".format(len(updated_records_ids)))

            # delete authority records from authority index by record id (deletes entries by record id and heading)
            self.remove_deleted_records_from_authority_index(deleted_records_ids, authority_index)

            # update authority records in authority index by record id (updates entries by record id and heading)
            self.update_updated_records_in_authority_index(updated_records_ids, authority_index)

            # set update status
            updater_status.update_in_progress = False
            updater_status.last_auth_update = date_to
            logging.info("Zmieniono status updatera na: {}".format(updater_status.update_in_progress))
        else:
            pass

    @staticmethod
    def update_updated_records_in_authority_index(updated_records_ids, authority_index):
        chunk_max_size = 100
        chunks = [updated_records_ids[i:i + chunk_max_size] for i in range(0, len(updated_records_ids), chunk_max_size)]

        for chunk in chunks:
            data = get_marc_authority_data_from_data_bn(chunk)

            rdr = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling='ignore')

            for rcd in rdr:
                for fld in AUTHORITY_INDEX_FIELDS:
                    if fld in rcd:
                        heading = get_rid_of_punctuation(rcd.get_fields(fld)[0].value())
                        dict_of_ids_to_append = prepare_dict_of_authority_ids_to_append(rcd)
                        nlp_id = get_nlp_id(rcd)

                        if nlp_id:

                            if nlp_id in authority_index:  # rcd is old and already indexed
                                old_heading = authority_index.get(nlp_id)
                                if old_heading == heading:  # heading wasn't modified - break and continue with next rcd
                                    break
                                else:  # heading was modified
                                    authority_index[nlp_id] = heading  # set new heading for this id

                                    old_heading_ids = authority_index[old_heading]  # get reference to old heading ids

                                    if len(old_heading_ids) > 1:  # there is more than one dict of ids
                                        del old_heading_ids[nlp_id]  # delete the obsolete dict of ids
                                        logging.debug(f'Usunięto zestaw id z (mod): {old_heading}')

                                        authority_index.setdefault(heading, {}).update(dict_of_ids_to_append)  # set new ids
                                        break
                                    else:  # there is only one dict of ids
                                        del authority_index[old_heading]  # delete entry completely
                                        logging.debug(f'Usunięto hasło całkowicie (mod): {old_heading}')

                                        authority_index.setdefault(heading, {}).update(dict_of_ids_to_append)  # set new ids
                                        break
                            else:  # rcd is new and it has to be indexed
                                authority_index[nlp_id] = heading
                                authority_index.setdefault(heading, {}).update(dict_of_ids_to_append)
                                logging.debug(f'Dodano nowe hasło (new): {heading}')
                                break

    @staticmethod
    def remove_deleted_records_from_authority_index(records_ids, authority_index):
        for record_id in records_ids:
            if record_id in authority_index:
                heading = authority_index.pop(record_id)
                heading_ids = authority_index[heading]
                if len(heading_ids) > 1:
                    heading_ids.remove(record_id)
                    logging.debug(f'Usunięto zestaw id z (del): {heading}')
                else:
                    del authority_index[heading]
                    logging.debug(f'Usunięto hasło całkowicie (del): {heading}')

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
