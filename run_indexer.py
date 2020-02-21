from indexer.authority_indexer import create_authority_index
from indexer.authority_external_ids_indexer import AuthorityExternalIdsIndex


#create_authority_index()
AuthorityExternalIdsIndex().index_in_redis()
