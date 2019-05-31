import requests
from pymarc import MARCReader
from datetime import datetime, timedelta
import logging

from utils.indexer_utils import get_nlp_id
from utils.marc_utils import get_marc_bibliographic_data_from_data_bn
from utils.updater_utils import get_nlp_id_from_json

from config.timedelta_config import TIMEDELTA_CONFIG


class BibUpdater(object):
    def __init__(self):
        pass

    def update_bibliographic_index(self, bib_index, updater_status):
        # set update status
        updater_status.update_in_progress = True
        logging.info("Zmieniono status updatera na: {}".format(updater_status.update_in_progress))

        # create dates for queries
        date_from = updater_status.last_bib_update - timedelta(days=TIMEDELTA_CONFIG)
        date_from_in_iso_with_z = date_from.isoformat(timespec='seconds') + 'Z'
        date_to = datetime.utcnow()
        date_to_in_iso_with_z = date_to.isoformat(timespec='seconds') + 'Z'

        # get deleted bib records ids from data.bn.org.pl
        deleted_query = 'http://data.bn.org.pl/api/bibs.json?updatedDate={}%2C{}&deleted=true&limit=100'.format(
            date_from_in_iso_with_z, date_to_in_iso_with_z)

        deleted_records_ids = self.get_records_ids_from_data_bn_for_bibliographic_index_update(deleted_query)
        logging.info("Rekordów bibliograficznych usuniętych: {}".format(len(deleted_records_ids)))

        # get updated bib records ids from data.bn.org.pl
        updated_query = 'http://data.bn.org.pl/api/bibs.json?updatedDate={}%2C{}&limit=100'.format(
            date_from_in_iso_with_z, date_to_in_iso_with_z)

        updated_records_ids = self.get_records_ids_from_data_bn_for_bibliographic_index_update(updated_query)
        logging.info("Rekordów bibliograficznych zmodyfikowanych: {}".format(len(updated_records_ids)))

        # delete authority records from authority index by record id (deletes entries by record id and heading)
        self.remove_deleted_records_from_bibliographic_index(deleted_records_ids, bib_index)

        # update authority records in authority index by record id (updates entries by record id and heading)
        self.update_updated_records_in_bibliographic_index(updated_records_ids, bib_index)

        # set update status
        updater_status.update_in_progress = False
        updater_status.last_bib_update = date_to
        logging.info("Zmieniono status updatera na: {}".format(updater_status.update_in_progress))

    @staticmethod
    def update_updated_records_in_bibliographic_index(updated_records_ids, bib_index):
        chunk_max_size = 100
        chunks = [updated_records_ids[i:i + chunk_max_size] for i in range(0, len(updated_records_ids), chunk_max_size)]

        for chunk in chunks:
            data = get_marc_bibliographic_data_from_data_bn(chunk)

            rdr = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling='ignore')
            for rcd in rdr:
                record_id = get_nlp_id(rcd)
                bib_index[record_id] = rcd.as_marc()

    @staticmethod
    def remove_deleted_records_from_bibliographic_index(records_ids, bib_index):
        for record_id in records_ids:
            bib_index.pop(record_id, None)

    @staticmethod
    def get_records_ids_from_data_bn_for_bibliographic_index_update(query):
        records_ids = []

        while query:
            r = requests.get(query)
            logging.debug("Pobieram: {}".format(query))
            json_chunk = r.json()

            for rcd in json_chunk['bibs']:
                record_id = get_nlp_id_from_json(rcd)
                records_ids.append(record_id)
                logging.debug("Dołączam rekord nr: {}".format(record_id))

            query = json_chunk['nextPage'] if json_chunk['nextPage'] else None

        return records_ids
