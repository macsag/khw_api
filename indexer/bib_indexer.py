from tqdm import tqdm
from pymarc import MARCReader

from utils.indexer_utils import get_nlp_id, get_mms_id


def create_bib_index(data, resolver_index=False):
    """
    Creates bibliographic records index in form of dictionary.
    Structure:
    record id (string): record (bytes)
    [thanks to bytes ('ISO transmission format') all bibs can be easily loaded to memory]

    Available requests: by bibliographic record nlp_id.

    Optionally creates resolver index.
    """
    bib_index = {}
    bib_resolver_index = {}

    with open(data, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore')
        for rcd in tqdm(rdr):
            nlp_id = get_nlp_id(rcd)
            bib_index[nlp_id] = rcd.as_marc()
            if resolver_index:
                create_bib_resolver_index(rcd, bib_resolver_index)

    return bib_index, bib_resolver_index


def create_bib_resolver_index(rcd, bib_resolver_index):
    mms_id = get_mms_id(rcd)
    nlp_id = get_nlp_id(rcd)

    value_to_index = {'mms_id': mms_id, 'nlp_id': nlp_id}

    if mms_id:
        bib_resolver_index[mms_id] = value_to_index
    if nlp_id:
        bib_resolver_index[nlp_id] = value_to_index
