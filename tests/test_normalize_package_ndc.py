import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# Importa funciones desde el script
from scripts.e_normalize_package_ndc import normalize_package_ndc

class TestNormalizePackageNDC(unittest.TestCase):

    def test_format_4_4_2(self):
        # XXXX-XXXX-XX -> Test para verificar patron 4-4-2
        input_ndc = "1234-5678-90"
        expected = "01234567890"
        self.assertEqual(normalize_package_ndc(input_ndc), expected)

    def test_format_5_3_2(self):
        # XXXXX-XXX-XX -> Test para verificar patron 5-3-2
        input_ndc = "12345-678-90"
        expected = "12345067890"
        self.assertEqual(normalize_package_ndc(input_ndc), expected)

    def test_format_5_4_1(self):
        # XXXXX-XXXX-X -> Test para verificar patron 5-4-1
        input_ndc = "12345-6789-0"
        expected = "12345678900"
        self.assertEqual(normalize_package_ndc(input_ndc), expected)

    def test_format_5_4_2(self):
        # XXXXX-XXXX-XX -> Test para verificar remoción de guiones
        input_ndc = "12345-6789-00"
        expected = "12345678900"
        self.assertEqual(normalize_package_ndc(input_ndc), expected)

    def test_invalid_format(self):
        # Formato invalido -> Test para verificar error por patron invalido
        input_ndc = "12-34-56"
        self.assertIsNone(normalize_package_ndc(input_ndc))

    def test_non_string_input(self):
        #  Formato invalido -> Test para verificar error por formato invalido
        self.assertIsNone(normalize_package_ndc(None))
        self.assertIsNone(normalize_package_ndc(123456))

if __name__ == "__main__":
    unittest.main()

# correr tests con:
# python -m unittest tests/test_normalize_package_ndc.py