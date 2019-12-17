def check_defg_034(coords) -> bool:
    is_valid = True
    errors = []

    for subfield in 'defg':
        if coords.get_subfields(subfield):
            if len(coords.get_subfields(subfield)) != 1:
                errors.append(f'Problem z podpolem {subfield} - '
                      f'występuje {len(coords.get_subfields(subfield))} razy.')
                is_valid = False
            else:
                if '.' in coords.get_subfields(subfield)[0]:
                    errors.append(f'Problem z podpolem {subfield} - '
                          f'zdaje się, że to już stopnie dziesiętne - {coords.get_subfields(subfield)[0]}.')
                    is_valid = False
                if len(coords.get_subfields(subfield)[0]) != 8:
                    errors.append(f'Problem z podpolem {subfield} - '
                          f'zła długość, powinno być 8, a jest {len(coords.get_subfields(subfield)[0])} - '
                          f'{coords.get_subfields(subfield)[0]}.')
                    is_valid = False
        else:
            errors.append(f'Problem z podpolem {subfield} - '
                  f'nie występuje.')
            is_valid = False

    return is_valid


def get_list_of_coords_from_valid_marc(coords):
    list_of_coords = []

    for subfield in 'defg':
        list_of_coords.append(dms_to_decimal(coords.get_subfields(subfield)[0]))

    return list_of_coords


def dms_to_decimal(single_coord):
    hemisphere = single_coord[0]
    d = single_coord[1:4]
    m = single_coord[4:6]
    s = single_coord[6:]
    sign = 1 if hemisphere in ['N', 'E'] else -1
    return (int(d) + int(m) / 60 + int(s) / 3600) * sign


def convert_to_bbox(coords):
    return f'{coords[0]},{coords[2]},{coords[1]},{coords[3]}'
