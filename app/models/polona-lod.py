from pydantic import BaseModel, HttpUrl


class PolonaLodV2Identifier(BaseModel):
    type: str
    display: str
    link: HttpUrl


class PolonaLodV2Subject(BaseModel):
    name: str
    identifiers: list[PolonaLodV2Identifier]


class PolonaLodV2Descriptor(BaseModel):
    name: str
    subjects: list[PolonaLodV2Subject]


class PolonaLodV2(BaseModel):
    descriptors: list[PolonaLodV2Descriptor]
