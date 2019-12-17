from typing import Optional


class AuthorityRecordsChunk(object):
    def __init__(self, authority_ids, local_auth_index, local_auth_external_ids_index):
        self.authority_ids = authority_ids
        self.json_processed_chunk = self.get_all_ids(local_auth_index, local_auth_external_ids_index)

    def get_all_ids(self, local_auth_index, local_auth_external_ids_index) -> dict:
        dict_to_return = {}

        try:
            authority_ids_list = self.authority_ids.split(',')
        except ValueError:
            authority_ids_list = [self.authority_ids]

        for authority_id in authority_ids_list:
            internal_ids_and_viaf = self.get_internal_ids_and_viaf(local_auth_index, authority_id)
            external_ids = self.get_external_ids(local_auth_external_ids_index, authority_id)
            merged_dict = self.merge_results(authority_id, internal_ids_and_viaf, external_ids)

            if merged_dict:
                dict_to_return.update(merged_dict)
            else:
                dict_to_return.setdefault(authority_id, None)

        return dict_to_return

    @staticmethod
    def get_internal_ids_and_viaf(local_auth_index: dict, authority_id: str) -> Optional[dict]:
        name = local_auth_index.get(authority_id)
        ids = local_auth_index.get(name) if name else None
        ids_to_return = ids.get(authority_id) if ids else None
        return ids_to_return if ids_to_return else None

    @staticmethod
    def get_external_ids(local_auth_external_ids_index, authority_id: str) -> Optional[dict]:
        ids = local_auth_external_ids_index.get_ids(authority_id)
        return ids if ids else None

    @staticmethod
    def merge_results(authority_id: str, internal_ids_and_viaf: dict, external_ids) -> Optional[dict]:
        result_dict = {}

        if internal_ids_and_viaf:
            result_dict.setdefault(authority_id, {}).update(internal_ids_and_viaf)
        if external_ids:
            result_dict.setdefault(authority_id, {}).update(external_ids)

        return result_dict if result_dict else None
