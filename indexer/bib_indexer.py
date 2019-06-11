from tqdm import tqdm
from pymarc import MARCReader

from utils.indexer_utils import get_nlp_id


def create_bib_index(data, resolver_index=False):
    """
    Creates bibliographic records index in form of dictionary.
    Structure:
    record id (string): record (bytes)
    [thanks to bytes ('ISO transmission format') all bibs can be easily loaded to memory]

    Available requests: by bibliographic record nlp_id.
    """
    bib_index = {}

    with open(data, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore')
        for rcd in tqdm(rdr):
            nlp_id = get_nlp_id(rcd)
            bib_index[nlp_id] = rcd.as_marc()

    return bib_index



