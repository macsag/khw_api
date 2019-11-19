import pymarc
from pymarc.exceptions import RecordLengthInvalid, RecordLeaderInvalid
from pymarc.exceptions import BaseAddressInvalid, BaseAddressNotFound
from pymarc.exceptions import RecordDirectoryInvalid, NoFieldsFound


class PermissiveMARCReader(pymarc.MARCReader):
    """PermissiveMARCReader: recovers from most pymarc exceptions"""

    def __init__(self, marc_target, to_unicode=False, force_utf8=False,
                 hide_utf8_warnings=False, utf8_handling='strict'):
        super(PermissiveMARCReader, self).__init__(marc_target, to_unicode,
                                                   force_utf8, hide_utf8_warnings, utf8_handling)
        self.count = 0
        self.failed = 0

    def next(self):
        """To support iteration."""
        pos = self.file_handle.tell()
        first5 = self.file_handle.read(5)
        if not first5:
            raise StopIteration
        if len(first5) < 5:
            raise RecordLengthInvalid

        length = int(first5)
        chunk = self.file_handle.read(length - 5)
        chunk = first5 + chunk
        try:
            record = pymarc.Record(chunk,
                                   to_unicode=self.to_unicode,
                                   force_utf8=self.force_utf8,
                                   hide_utf8_warnings=self.hide_utf8_warnings,
                                   utf8_handling=self.utf8_handling)
            self.count += 1
            return record
        except (RecordLeaderInvalid, BaseAddressNotFound, BaseAddressInvalid,
                RecordDirectoryInvalid, NoFieldsFound, UnicodeDecodeError):
            self.file_handle.seek(pos + length)
            self.count += 1
            self.failed += 1
            pass
