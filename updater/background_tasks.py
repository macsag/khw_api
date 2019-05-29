def do_authority_update(updater_instance, auth_index, updater_status_instance):
    updater_instance.update_authority_index(auth_index, updater_status_instance)


def do_bib_update(updater_instance, bib_index, updater_status_instance):
    updater_instance.update_bibliographic_index(bib_index, updater_status_instance)
