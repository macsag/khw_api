import logging
import os
from pathlib import Path
import sys

from dotenv import load_dotenv

from indexers.authority_indexer import index_authorities_in_redis


load_dotenv()


DB_AUTHORITY_DUMP_PATH = '..' / Path.cwd() / os.getenv('DB_AUTHORITY_DUMP_PATH')


# logging settings
default_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
duplicates_formatter = logging.Formatter('%(message)s')

f_indexer_handler = logging.FileHandler('indexer.log', encoding='utf-8')
f_all_duplicates_handler = logging.FileHandler('all_duplicates.log', encoding='utf-8')
f_interfield_duplicates_handler = logging.FileHandler('interfield_duplicates.log', encoding='utf-8')
f_intrafield_duplicates_handler = logging.FileHandler('intrafield_duplicates.log', encoding='utf-8')

str_handler = logging.StreamHandler(sys.stdout)

f_indexer_handler.setFormatter(default_formatter)
f_all_duplicates_handler.setFormatter(duplicates_formatter)
f_interfield_duplicates_handler.setFormatter(duplicates_formatter)
f_intrafield_duplicates_handler.setFormatter(duplicates_formatter)

str_handler.setFormatter(default_formatter)


logger_all_duplicates = logging.getLogger('logger_all_duplicates')
logger_all_duplicates.addHandler(f_all_duplicates_handler)

logger_interfield_duplicates = logging.getLogger('logger_interfield_duplicates')
logger_interfield_duplicates.addHandler(f_interfield_duplicates_handler)

logger_intrafield_duplicates = logging.getLogger('logger_intrafield_duplicates')
logger_intrafield_duplicates.addHandler(f_intrafield_duplicates_handler)

logging.root.addHandler(str_handler)
logging.root.addHandler(f_indexer_handler)
logging.root.setLevel(level=logging.INFO)

# run indexer
index_authorities_in_redis(DB_AUTHORITY_DUMP_PATH)
#AuthorityExternalIdsIndex().index_in_redis()
