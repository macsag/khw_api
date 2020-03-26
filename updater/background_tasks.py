async def do_authority_update(updater_instance, aiohttp_session, conn_auth_int):
    await updater_instance.update_authority_index(aiohttp_session, conn_auth_int)
