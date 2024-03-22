import logging
import sys

from indexer.authority_indexer import create_authority_index, flush_db
from indexer.authority_external_ids_indexer import AuthorityExternalIdsIndex

# set up logging
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fhandler = logging.FileHandler('indexer.log', encoding='utf-8')
f_interfield_duplicates_handler = logging.FileHandler('interfield_duplicates.log', encoding='utf-8')
strhandler = logging.StreamHandler(sys.stdout)

fhandler.setFormatter(formatter)
f_interfield_duplicates_handler.setFormatter(formatter)
strhandler.setFormatter(formatter)

logger_interfield_duplicates = logging.getLogger('logger_interfield_duplicates')
logger_interfield_duplicates.addHandler(f_interfield_duplicates_handler)
logging.root.addHandler(strhandler)
logging.root.addHandler(fhandler)
logging.root.setLevel(level=logging.INFO)

flush_db()
create_authority_index()
#AuthorityExternalIdsIndex().index_in_redis()
