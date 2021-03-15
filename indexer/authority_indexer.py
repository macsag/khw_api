import logging
import ujson
from pathlib import Path

from tqdm import tqdm
from pymarc import MARCReader
import redis

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from utils.indexer_utils import get_nlp_id, get_mms_id, get_viaf_id, get_coordinates
from utils.marc_utils import prepare_name_for_indexing


logger = logging.getLogger(__name__)

PATH_TO_DB = Path.cwd() / 'nlp_database' / 'production' / 'authorities-all.marc'


def flush_db() -> None:
    r = redis.Redis(db=8)  # use db=8 to avoid conflicts with test local dev Redis instance
    r.flushdb()
    r.close()


def create_authority_index(data: Path = PATH_TO_DB) -> None:
    logger.info('Rozpoczęto indeksowanie rekordów wzorcowych...')
    authority_count = 0

    r = redis.Redis(db=8)

    helper_dict = {}      # stores all authorities during initial indexing to disambiguate duplicates
    buff = {}             # used for batch indexing in Redis

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

                    # heading_tag is used to disambiguate authority duplicates
                    # if duplicate is detected, authorities with tag 130 are ignored
                    serialized_to_dict = {'nlp_id': nlp_id,
                                          'mms_id': mms_id,
                                          'viaf_id': viaf_id,
                                          'coords': coordinates,
                                          'heading': heading_full,
                                          'heading_tag': fld}

                    serialized_to_json = ujson.dumps(serialized_to_dict, ensure_ascii=False)

                    # check if not duplicate using heading_tag
                    # least desirable tag: 130
                    descr_from_helper = helper_dict.get(heading_to_index)

                    if descr_from_helper:
                        if descr_from_helper.get('heading_tag') != '130' and fld == '130':
                            # if authority with the same heading is already indexed
                            # and the new one (currently processed) is 130 heading
                            # just skip it (break the loop) and log the event

                            logger.error(f'Dublet: {descr_from_helper.get("heading")} - '
                                         f'{descr_from_helper.get("heading_tag")} - '
                                         f'{descr_from_helper.get("nlp_id")} - '
                                         f'{descr_from_helper.get("mms_id")} || '
                                         f'{heading_full} - '
                                         f'{fld} - '
                                         f'{nlp_id} - '
                                         f'{mms_id}.')
                            break  # breaks only the inner loop (searching for fields to index)

                        else:
                            # if authority with the same heading is already indexed
                            # and the new one (currently processed) is not 130 heading
                            # log the event and do the usual job
                            # record will be normally processed and will overwrite the existing record

                            logger.error(f'Dublet: {descr_from_helper.get("heading")} - '
                                         f'{descr_from_helper.get("heading_tag")} - '
                                         f'{descr_from_helper.get("nlp_id")} - '
                                         f'{descr_from_helper.get("mms_id")} || '
                                         f'{heading_full} - '
                                         f'{fld} - '
                                         f'{nlp_id} - '
                                         f'{mms_id}.')

                            helper_dict.update({heading_to_index: serialized_to_dict,
                                                nlp_id: serialized_to_dict})

                    else:
                        helper_dict.update({heading_to_index: serialized_to_dict,
                                            nlp_id: serialized_to_dict})

                    buff.update({heading_to_index: serialized_to_json,
                                 nlp_id: serialized_to_json})

                    authority_count += 1
                    break

            if len(buff) > 1000:
                # index records in chunks by 1000
                r.mset(buff)
                buff.clear()

        if buff:
            # index records remaining in buffer
            r.mset(buff)

    r.close()

    logger.info(f'Zakończono indeksowanie rekordów wzorcowych. Zaindeksowano: {authority_count}.')
