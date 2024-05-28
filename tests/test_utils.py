import unittest


class TestDMSToDecimal(unittest.TestCase):

    def test_dms_to_decimal_E(self):
        data = 'E0183300'
        result = app.utils.coordinates_utils.dms_to_decimal(data)
        self.assertEqual(result, 18.55)

    def test_dms_to_decimal_W(self):
        data = 'W0183300'
        result = app.utils.coordinates_utils.dms_to_decimal(data)
        self.assertEqual(result, -18.55)

