def get_nlp_id_from_json(rcd):
    for fld in rcd['marc']['fields']:
        if fld.get('001'):
            return fld.get('001')
