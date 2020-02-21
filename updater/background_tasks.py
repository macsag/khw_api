async def do_authority_update(updater_instance, conn_auth_int, updater_status_instance):
    await updater_instance.update_authority_index(conn_auth_int, updater_status_instance)


def do_bib_update(updater_instance, bib_index, updater_status_instance):
    updater_instance.update_bibliographic_index(bib_index, updater_status_instance)
