from indexer.authority_indexer import create_authority_index, flush_db
from indexer.authority_external_ids_indexer import AuthorityExternalIdsIndex

flush_db()
create_authority_index()
AuthorityExternalIdsIndex().index_in_redis()
