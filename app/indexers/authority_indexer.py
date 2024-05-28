from collections import namedtuple
import logging
from pathlib import Path
import ujson

from pymarc import MARCReader, Record, Field
import redis
from tqdm import tqdm

from utils.indexer_consts import AUTHORITY_INDEX_FIELDS
from utils.indexer_utils import get_nlp_id, get_mms_id, get_viaf_id, get_p_id, get_coordinates
from utils.marc_utils import prepare_name_for_indexing


logger = logging.getLogger(__name__)

logger_all_duplicates = logging.getLogger('logger_all_duplicates')
logger_intrafield_duplicates = logging.getLogger('logger_intrafield_duplicates')
logger_interfield_duplicates = logging.getLogger('logger_interfield_duplicates')


def index_authorities_in_redis(
        db_authority_dump_path: Path,
        redis_db: int = 8,
        flush_db: bool = False) -> None:
    logger.info(f'Connecting to Redis, {redis_db=}...')
    r = redis.Redis(db=redis_db)
    logger.info(f'Connected.')

    if flush_db:
        r.flushdb()
        logger.info(f'Succesfully flushed {redis_db=}.')

    logger.info('Started indexing authorities...')
    authority_processed_count = 0
    authority_sent_to_redis_count = 0

    helper_dict = {}      # stores all authorities during initial indexing to disambiguate duplicates
    buff = {}             # used for batch indexing in Redis

    with open(str(db_authority_dump_path), 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)

        for rcd in tqdm(rdr):
            for fld in AUTHORITY_INDEX_FIELDS:
                if fld in rcd:
                    auth_rcd_to_index = prepare_authority_record_for_indexing(rcd, fld)
                    heading_to_index = prepare_name_for_indexing(auth_rcd_to_index.heading)

                    # check if not duplicate using heading_tag
                    # least desirable tag: 130
                    auth_rcd_from_helper = helper_dict.get(heading_to_index)
                    if auth_rcd_from_helper:
                        if auth_rcd_from_helper.heading_tag != '130' and fld == '130':
                            # if authority with the same heading is already indexed
                            # and the new one (currently processed) has 130 heading
                            # just skip it (break the loop) and log the event
                            # we rather want records with other headings to be indexed
                            # it is so called "interfield duplicate" with 130 heading (quite unique case)

                            logger_all_duplicates.error(prepare_duplicate_error_message(auth_rcd_from_helper,
                                                                                        auth_rcd_to_index,
                                                                                        heading_to_index))
                            logger_interfield_duplicates.error(prepare_duplicate_error_message(auth_rcd_from_helper,
                                                                                               auth_rcd_to_index,
                                                                                               heading_to_index))

                            break  # breaks only the inner loop (stops searching for fields to index)

                        else:
                            if auth_rcd_from_helper.heading_tag == fld:
                                # if authority with the same heading is already indexed
                                # and they have the same heading_tags
                                # check which one has the p_id, leave the one with the p_id and log the event
                                # we prefer records equipped with p_id
                                # if both records have p_id, leave the already indexed one -
                                # - (cause it's more effective to do that)
                                # it is so called "intrafield duplicate"

                                logger_all_duplicates.error(prepare_duplicate_error_message(auth_rcd_from_helper,
                                                                                            auth_rcd_to_index,
                                                                                            heading_to_index))
                                logger_intrafield_duplicates.error(
                                    prepare_duplicate_error_message(auth_rcd_from_helper,
                                                                    auth_rcd_to_index,
                                                                    heading_to_index))

                                if auth_rcd_from_helper.p_id:
                                    break  # breaks only the inner loop (stops searching for fields to index)
                                else:
                                    helper_dict[heading_to_index] = auth_rcd_to_index
                                    buff[heading_to_index] = auth_rcd_to_index.as_json()
                                    authority_sent_to_redis_count += 1
                                    break  # breaks only the inner loop (stops searching for fields to index)

                            else:
                                # if authority with the same heading is already indexed
                                # and they have different heading_tags
                                # check which one has the p_id, leave the one with the p_id and log the event
                                # we prefer records equipped with p_id
                                # if both records have p_id, leave the already indexed one -
                                # - (cause it's more effective to do that)
                                # it is yet another case of so called "interfield duplicate"

                                logger_all_duplicates.error(prepare_duplicate_error_message(auth_rcd_from_helper,
                                                                                            auth_rcd_to_index,
                                                                                            heading_to_index))
                                logger_interfield_duplicates.error(prepare_duplicate_error_message(auth_rcd_from_helper,
                                                                                                   auth_rcd_to_index,
                                                                                                   heading_to_index))

                                if auth_rcd_from_helper.p_id:
                                    break  # breaks only the inner loop (stops searching for fields to index)
                                else:
                                    helper_dict[heading_to_index] = auth_rcd_to_index
                                    buff[heading_to_index] = auth_rcd_to_index.as_json()
                                    authority_sent_to_redis_count += 1
                                    break  # breaks only the inner loop (stops searching for fields to index)

                    else:
                        # currently processed authority record is new, it has no duplicates
                        helper_dict[heading_to_index] = auth_rcd_to_index
                        buff[heading_to_index] = auth_rcd_to_index.as_json()
                        authority_sent_to_redis_count += 1

                else:
                    # there is no indexable 1XX field in the record
                    logger.error(f'No indexable field in {auth_rcd_to_index.nlp_id} / {auth_rcd_to_index.mms_id}.')

            authority_processed_count += 1

            if len(buff) > 1000:
                # index records in chunks by 1000
                r.mset(buff)
                buff.clear()

        if buff:
            # index records remaining in buffer
            r.mset(buff)

    logger.info(f'Done. Processed: {authority_processed_count}, sent to redis {authority_sent_to_redis_count}.')

    r.close()

    logger.info(f'Connection to Redis closed. ')


class AuthorityRecordForIndexing(namedtuple('AuthorityRecordForIndexing',
                                            ['heading',
                                             'heading_tag',
                                             'nlp_id',
                                             'mms_id',
                                             'p_id',
                                             'viaf_id',
                                             'coords'])):
    __slots__ = ()

    def as_json(self) -> str:
        return ujson.dumps(self._asdict(), ensure_ascii=False)


def prepare_authority_record_for_indexing(rcd: Record, fld: Field) -> AuthorityRecordForIndexing:
    heading = rcd.get_fields(fld)[0].value()
    heading_tag = fld
    nlp_id = get_nlp_id(rcd)
    mms_id = get_mms_id(rcd)
    p_id = get_p_id(rcd)
    viaf_id = get_viaf_id(rcd)
    coordinates = get_coordinates(rcd)

    return AuthorityRecordForIndexing(heading,
                                      heading_tag,
                                      nlp_id,
                                      mms_id,
                                      p_id,
                                      viaf_id,
                                      coordinates)


def prepare_duplicate_error_message(auth_rcd_from_helper: AuthorityRecordForIndexing,
                                    auth_rcd_to_index: AuthorityRecordForIndexing,
                                    heading_to_index: str) -> str:
    return (f'POSSIBLE DUPLICATE DETECTED: {heading_to_index}\n'
            f'    {auth_rcd_from_helper.heading_tag}'
            f' - <<{auth_rcd_from_helper.heading}>>'
            f' - [{auth_rcd_from_helper.nlp_id}'
            f' / {auth_rcd_from_helper.mms_id}'
            f' / {auth_rcd_from_helper.p_id if auth_rcd_from_helper.p_id else "NO P_ID"}\n'
            f'    {auth_rcd_to_index.heading_tag}'
            f' - <<{auth_rcd_to_index.heading}>>'
            f' - [{auth_rcd_to_index.nlp_id}'
            f' / {auth_rcd_to_index.mms_id}'
            f' / {auth_rcd_to_index.p_id if auth_rcd_from_helper.p_id else "NO P_ID"}\n')
