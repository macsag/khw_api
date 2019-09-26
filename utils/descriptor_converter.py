descriptor_types = {'personal_descriptor': '100', 'corporate_descriptor': '110', 'subject_descriptor': '150'}


def convert_descriptor(descriptor_marc_object, descr_types):
    descriptor_type = get_descriptor_type(descriptor_marc_object, descr_types)

    return convert_by_type(descriptor_marc_object, descriptor_type)


def get_descriptor_type(descriptor_marc_object, descr_types):
    for descr_type, marc_tag in descr_types.items():
        if descriptor_marc_object.get_fields(marc_tag):
            return descr_type
    else:
        raise Exception('This is not a valid descriptor!')


def convert_by_type(descriptor_marc_object, descriptor_type):
    if descriptor_type == 'subject_descriptor':
        return convert_subject_descriptor(descriptor_marc_object)


def convert_subject_descriptor(descriptor_marc_object):

    descriptor_preferred_name = [v.value() for v in descriptor_marc_object.get_fields('150')]
    descriptor_alt_names = [v.value() for v in descriptor_marc_object.get_fields('450')]

    converted_descriptor = {'descriptor_preferred_name': descriptor_preferred_name,
                            'descriptor_alt_names': descriptor_alt_names}

    return converted_descriptor



