import logging
import sys

from indexer.authority_indexer import create_authority_index, flush_db
from indexer.authority_external_ids_indexer import AuthorityExternalIdsIndex

# set up logging
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fhandler = logging.FileHandler('indexer.log', encoding='utf-8')
strhandler = logging.StreamHandler(sys.stdout)
fhandler.setFormatter(formatter)
strhandler.setFormatter(formatter)

logging.root.addHandler(strhandler)
logging.root.addHandler(fhandler)
logging.root.setLevel(level=logging.INFO)


flush_db()
create_authority_index()
AuthorityExternalIdsIndex().index_in_redis()
