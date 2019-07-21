from tqdm import tqdm
from pymarc import MARCReader
import logging

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from utils.indexer_utils import get_nlp_id, get_mms_id
from utils.marc_utils import get_rid_of_punctuation


def create_authority_index(data):
    """
    Create authority records index in form of dictionary.
    Structure: heading (string): record ids (list of dict of ids).

    Available requests: by authority heading; by authority nlp_id.
    """
    authority_index = {}

    with open(data, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore')
        for rcd in tqdm(rdr):
            for fld in AUTHORITY_INDEX_FIELDS:
                if fld in rcd:
                    heading = get_rid_of_punctuation(rcd.get_fields(fld)[0].value())
                    nlp_id = get_nlp_id(rcd)
                    mms_id = get_mms_id(rcd)

                    authority_index.setdefault(heading, {}).update({nlp_id: mms_id})
                    authority_index[nlp_id] = heading
                    logging.debug(f'Zaindeksowano: {heading} | {authority_index[heading]}')
                    logging.debug(f'Zaindeksowano: {nlp_id} | {heading}')

    return authority_index
