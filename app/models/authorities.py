from pydantic import BaseModel


class AuthorityRecord(BaseModel):
    ids_from_internal: dict
    ids_from_external: dict
