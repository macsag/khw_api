import logging
import json
from pathlib import Path

from tqdm import tqdm
from pymarc import MARCReader
import redis

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from utils.indexer_utils import get_nlp_id, get_mms_id, get_viaf_id, get_coordinates
from utils.marc_utils import prepare_name_for_indexing

logger = logging.getLogger(__name__)

PATH_TO_DB = Path.cwd() / 'nlp_database' / 'production' / 'authorities-all.marc'


def create_authority_index(data=PATH_TO_DB):
    logger.info('Rozpoczęto indeksowanie rekordów wzorcowych.')
    authority_count = 0

    r = redis.Redis()

    with open(str(data), 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)
        for rcd in tqdm(rdr):
            for fld in AUTHORITY_INDEX_FIELDS:
                if fld in rcd:
                    heading_full = rcd.get_fields(fld)[0].value()
                    heading_to_index = prepare_name_for_indexing(heading_full)
                    nlp_id = get_nlp_id(rcd)
                    mms_id = get_mms_id(rcd)
                    viaf_id = get_viaf_id(rcd)
                    coordinates = get_coordinates(rcd)

                    serialized_to_dict = {'nlp_id': nlp_id,
                                          'mms_id': mms_id,
                                          'viaf_id': viaf_id,
                                          'coords': coordinates,
                                          'heading': heading_full}
                    serialized_to_json = json.dumps(serialized_to_dict, ensure_ascii=False)

                    r.mset(heading_to_index, serialized_to_json,
                           nlp_id, serialized_to_json)

                    logger.debug(f'Zaindeksowano: {heading_to_index}.')

                    authority_count += 1

    logger.info('Zakończono indeksowanie rekordów wzorcowych.')
    logger.info(f'Zaindeksowano rekordów wzorcowych: {authority_count}.')

