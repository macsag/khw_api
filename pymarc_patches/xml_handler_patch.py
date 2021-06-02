import unicodedata

from pymarc import XmlHandler
from pymarc import MARC_XML_NS
from pymarc import parse_xml

from pymarc.field import Field
from pymarc.record import Record


class XmlHandlerPatched(XmlHandler):
    def startElementNS(self, name, qname, attrs):
        if self._strict and name[0] != MARC_XML_NS:
            return

        element = name[1]
        self._text = []

        if element == 'record':
            self._record = Record()
        elif element == 'controlfield':
            tag = attrs.getValue((None, u'tag'))
            # patch 1: if it is not a valid controlfield, omit it
            if not tag < '010' or not tag.isdigit():
                return
            self._field = Field(tag)
        elif element == 'datafield':
            tag = attrs.getValue((None, u'tag'))
            # patch 3: if somehow datafield has controlfield tag
            if tag in ['001', '002', '003', '004', '005', '006', '007', '008', '009', '04 ']:
                return
            ind1 = attrs.get((None, u'ind1'), u' ')
            ind2 = attrs.get((None, u'ind2'), u' ')
            # patch 2: if field lacks indicators or they're not ASCII, force to blank
            try:
                self._field = Field(tag, [ind1, ind2])
            except (ValueError, UnicodeEncodeError):
                self._field = Field(tag, [' ', ' '])
        elif element == 'subfield':
            self._subfield_code = attrs[(None, 'code')]

    def endElementNS(self, name, qname):
        if self._strict and name[0] != MARC_XML_NS:
            return

        element = name[1]
        if self.normalize_form is not None:
            text = unicodedata.normalize(self.normalize_form, u''.join(self._text))
        else:
            text = u''.join(self._text)

        if element == 'record':
            self.process_record(self._record)
            self._record = None
        elif element == 'leader':
            self._record.leader = text
        elif element == 'controlfield':
            # patch 1: if it is not a valid controlfield, omit it
            if not self._field:
                return
            self._field.data = text
            self._record.add_field(self._field)
            self._field = None
        elif element == 'datafield':
            # patch 3: if it is not a valid datafield, omit it
            if not self._field:
                return
            self._record.add_field(self._field)
            self._field = None
        elif element == 'subfield':
            print(self._field)
            # patch 3: if it is a subfield within an invalid datafield, omit it
            if not self._field:
                return
            self._field.subfields.append(self._subfield_code)
            self._field.subfields.append(text)
            self._subfield_code = None

        self._text = []


def parse_xml_to_array_patched(xml_file, strict=False, normalize_form=None):
    """
    parse an xml file and return the records as an array. If you would
    like the parser to explicitly check the namespaces for the MARCSlim
    namespace use the strict=True option.
    Valid values for normalize_form are 'NFC', 'NFKC', 'NFD', and 'NFKD'. See
    unicodedata.normalize info.
    """
    handler = XmlHandlerPatched(strict, normalize_form)
    parse_xml(xml_file, handler)
    return handler.records
