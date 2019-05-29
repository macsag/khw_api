class UpdaterStatus(object):
    def __init__(self, date_now):
        self.update_in_progress = False

        self.last_bib_update = date_now
        self.last_auth_update = date_now
