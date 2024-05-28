class AuthorityUpdaterClient(object):
    _authority_updater: AuthorityUpdater = None

    @classmethod
    def _create_authority_updater(cls):
        cls._authority_updater = AuthorityUpdater()

    @classmethod
    def get_authority_updater(cls):
        if not cls._authority_updater:
            cls._create_authority_updater()
        return cls._authority_updater