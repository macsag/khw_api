import requests


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


def prepare_dict_of_authority_ids_to_append(rcd):
    mms_id = get_mms_id(rcd)
    nlp_id = get_nlp_id(rcd)

    dict_of_ids_to_append = {'mms_id': mms_id, 'nlp_id': nlp_id}

    return dict_of_ids_to_append


def is_data_bn_ok():
    return True if requests.get('http://data.bn.org.pl').status_code == 200 else False
