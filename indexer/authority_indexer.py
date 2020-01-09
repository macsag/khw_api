from tqdm import tqdm
from pymarc import MARCReader
import logging

from config.indexer_config import AUTHORITY_INDEX_FIELDS
from utils.indexer_utils import get_nlp_id, get_mms_id, get_viaf_id, get_coordinates
from utils.marc_utils import prepare_name_for_indexing


def create_authority_index(data):
    """
    Create authority records index in form of dictionary.
    Structure: heading (string): record ids (list of dict of ids).

    Available requests: by authority heading; by authority nlp_id.
    """
    authority_index = {}

    with open(data, 'rb') as fp:
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

                    serialized_to_dict = {}
                    serialized_to_dict.update({nlp_id: {'mms_id': mms_id,
                                                        'viaf_id': viaf_id,
                                                        'coords': coordinates,
                                                        'heading': heading_full}})

                    authority_index.setdefault(heading_to_index, {}).update(serialized_to_dict)
                    authority_index[nlp_id] = heading_to_index
                    logging.debug(f'Zaindeksowano: {heading_to_index} | {authority_index[heading_to_index]}')
                    logging.debug(f'Zaindeksowano: {nlp_id} | {heading_to_index}')

    return authority_index
