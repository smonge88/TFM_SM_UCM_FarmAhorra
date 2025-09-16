import unittest
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Importa funciones desde el script
from scripts.b_clean_nadac import clean_nadac

# Paths y archivos preestablecidos para ejecución de los tests
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "test_nadac.csv")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
CLEANED_PATH = os.path.join(CLEANED_DIR, "nadac_clean.csv")

class TestCleanNadac(unittest.TestCase):

    def setUp(self):
        # Crea carpeta si no existe
        os.makedirs(os.path.join(BASE_DIR, "data", "raw"), exist_ok=True)

        # Crea CSV con datos de prueba
        data = {
            "NDC": ["01234567890", "12345678901", None, "1234567890"],
            "As of Date": ["05/01/2025", "05/01/2025", "invalid", "05/01/2025"],
            "NADAC Per Unit": [0.50, None, 0.70, 0.90],
            "Pharmacy Type Indicator": ["X", "Y", "Z", "A"],
            "Explanation Code": ["A", "B", "C", "D"],
            "Classification for Rate Setting": ["AA", "BB", "CC", "DD"],
            "Corresponding Generic Drug NADAC Per Unit": [1.0, 2.0, 3.0, 4.0],
            "Corresponding Generic Drug Effective Date": ["04/01/2025", "04/01/2025", "04/01/2025", "04/01/2025"]
        }

        # Guarda csv de prueba en carpeta Raw como test_nadac.csv
        df = pd.DataFrame(data)
        df.to_csv(RAW_PATH, index=False)

    # Función para limpiar archivos temporales
    def tearDown(self):
        if os.path.exists(RAW_PATH):
            os.remove(RAW_PATH)
        if os.path.exists(CLEANED_PATH):
            os.remove(CLEANED_PATH)
        if os.path.isdir(CLEANED_DIR):
            try:
                os.rmdir(CLEANED_DIR)
            except OSError:
                pass  # si tiene archivos, lo ignora

    # Función que ejecuta el clean_nadac del set de prueba
    def test_clean_nadac_output(self):
        df_clean = clean_nadac(path=RAW_PATH)

        # Test 1: Verifica que el resultado no sea vacío
        self.assertIsInstance(df_clean, pd.DataFrame)
        self.assertGreater(len(df_clean), 0)

        # Test 2: Verifica que solo hay NDCs con longitud 11
        self.assertTrue(all(df_clean["NDC"].str.len() == 11))

        # Test 3: Verifica que las columnas eliminadas ya no estén
        eliminated_cols = [
            'Pharmacy Type Indicator',
            'Explanation Code',
            'Classification for Rate Setting',
            'Corresponding Generic Drug NADAC Per Unit',
            'Corresponding Generic Drug Effective Date'
        ]
        for col in eliminated_cols:
            self.assertNotIn(col, df_clean.columns)

        # Test 4: Verifica que el archivo limpio se haya guardado
        self.assertTrue(os.path.exists(CLEANED_PATH))

if __name__ == "__main__":
    unittest.main()

# correr script de pytest desde la terminal con:
# python -m unittest tests/test_clean_nadac.py