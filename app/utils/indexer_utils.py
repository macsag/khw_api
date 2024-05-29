from collections import namedtuple
from typing import Optional

from pymarc import Record, Field
import ujson

from coordinates_utils import check_defg_034, get_list_of_coords_from_valid_marc, convert_to_bbox


def get_mms_id(rcd):
    return rcd.get_fields('009')[0].value() if rcd.get_fields('009') else None


def get_nlp_id(rcd):
    return rcd.get_fields('001')[0].value()


def get_viaf_id(rcd):
    f_024 = rcd.get_fields('024')
    if f_024:
        for f in f_024:
            s_2 = f.get_subfields('2')
            if s_2:
                if s_2[0] == 'viaf':
                    if f.get_subfields('a'):
                        return f.get_subfields('a')[0]
    return None


def get_p_id(rcd):
    return rcd.get_fields('010')[0].value() if rcd.get_fields('010') else None


def get_coordinates(rcd) -> Optional[str]:
    coords_034 = rcd.get_fields('034')[0] if rcd.get_fields('034') else None
    if coords_034:
        if check_defg_034(coords_034):
            try:
                coords_list = get_list_of_coords_from_valid_marc(coords_034)
            except ValueError:
                return None
            return convert_to_bbox(coords_list)
    return None


async def is_data_bn_ok(aiohttp_session):
    async with aiohttp_session.get('http://data.bn.org.pl') as response:
        if response.status == 200:
            return True
        else:
            return None


class AuthorityRecordForIndexing(namedtuple('AuthorityRecordForIndexing',
                                            ['heading',
                                             'heading_tag',
                                             'nlp_id',
                                             'mms_id',
                                             'p_id',
                                             'viaf_id',
                                             'coords'])):
    __slots__ = ()

    def as_json(self) -> str:
        return ujson.dumps(self._asdict(), ensure_ascii=False)


def prepare_authority_record_for_indexing(rcd: Record, fld: Field) -> AuthorityRecordForIndexing:
    heading = rcd.get_fields(fld)[0].value()
    heading_tag = fld
    nlp_id = get_nlp_id(rcd)
    mms_id = get_mms_id(rcd)
    p_id = get_p_id(rcd)
    viaf_id = get_viaf_id(rcd)
    coordinates = get_coordinates(rcd)

    return AuthorityRecordForIndexing(heading,
                                      heading_tag,
                                      nlp_id,
                                      mms_id,
                                      p_id,
                                      viaf_id,
                                      coordinates)
