import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Importa funciones desde el script
from scripts.f_add_selling_size import extract_selling_size

class TestExtractSellingSize(unittest.TestCase):

    def test_entero_inicio(self): # Test de lectura de números enteros
        self.assertEqual(extract_selling_size("100 mg in 1 vial"), 100.0)
        self.assertEqual(extract_selling_size("5 tablets per box"), 5.0)

    def test_decimal_con_cero(self): # Test de lectura de números decimales
        self.assertEqual(extract_selling_size("0.25 kg in 1 container"), 0.25)
        self.assertEqual(extract_selling_size("3.5 mL"), 3.5)

    def test_decimal_sin_cero(self): # Test de lectura de números decimales sin dígito antes de la coma
        self.assertEqual(extract_selling_size(".75 oz"), 0.75)
        self.assertEqual(extract_selling_size(".1 mg/mL"), 0.1)

    def test_texto_sin_numero_inicio(self): # Test de lectura de descripción invalida sin numero al inicio
        self.assertIsNone(extract_selling_size("mg per vial"))
        self.assertIsNone(extract_selling_size("One bottle per box"))

    def test_valores_invalidos(self): # Test de lectura de descripciones inválidas
        self.assertIsNone(extract_selling_size(""))
        self.assertIsNone(extract_selling_size(None))
        self.assertIsNone(extract_selling_size(123))  # input no string

    def test_espacios_antes_del_numero(self): # Test de lectura de descripciones alternativas con espacio al inicio
        self.assertEqual(extract_selling_size("  250 g in 1 pouch"), 250.0)
        self.assertEqual(extract_selling_size("\t0.5 L"), 0.5)

if __name__ == '__main__':
    unittest.main()

# correr test con:
# python -m unittest tests/test_extract_selling_size.py
