import requests


def search_in_data_bn(search_phr):
    query = f'http://data.bn.org.pl/api/authorities.json?subject={search_phr}'
    return requests.get(query).json()


def get_fields_from_json(descriptor_json_object, field_tag):
    fld_list = []
    for fld in descriptor_json_object['marc']['fields']:
        print(fld)
        print(type(fld))
        tag_value = fld.get(field_tag)
        if tag_value:
            tag_sbflds = tag_value['subfields']
            if tag_sbflds:
                for sbfld in tag_sbflds:
                    value_to_append = ' '.join([sbfld_value for sbfld_value in sbfld.values()])
            fld_list.append(value_to_append)
    return fld_list


def get_descriptor_type_from_json(descriptor_json_object, descr_types):
    for descr_type, marc_tag in descr_types.items():
        if get_fields_from_json(descriptor_json_object, marc_tag):
            return descr_type
    else:
        raise Exception('This is not a valid descriptor!')
