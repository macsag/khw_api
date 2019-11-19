from tqdm import tqdm
from utils.permissive_marc_reader import PermissiveMARCReader
import logging

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from utils.indexer_utils import get_nlp_id, get_mms_id, get_viaf_id
from utils.marc_utils import prepare_name_for_indexing


def create_authority_index(data):
    """
    Create authority records index in form of dictionary.
    Structure: heading (string): record ids (list of dict of ids).

    Available requests: by authority heading; by authority nlp_id.
    """
    authority_index = {}

    with open(data, 'rb') as fp:
        rdr = PermissiveMARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore')
        for rcd in tqdm(rdr):
            for fld in AUTHORITY_INDEX_FIELDS:
                if fld in rcd:
                    heading = prepare_name_for_indexing(rcd.get_fields(fld)[0].value())
                    nlp_id = get_nlp_id(rcd)
                    mms_id = get_mms_id(rcd)
                    viaf_id = get_viaf_id(rcd)

                    authority_index.setdefault(heading, {}).update({nlp_id: {'mms_id': mms_id, 'viaf_id': viaf_id}})
                    authority_index[nlp_id] = heading
                    logging.debug(f'Zaindeksowano: {heading} | {authority_index[heading]}')
                    logging.debug(f'Zaindeksowano: {nlp_id} | {heading}')

    return authority_index
