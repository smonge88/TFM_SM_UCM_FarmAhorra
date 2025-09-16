import unittest
from unittest.mock import patch, MagicMock
from requests.exceptions import ConnectionError
import os
import io
import zipfile
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Importa funciones desde el script
from scripts.a_download_sets import download_nadac, download_ndc, DATA_RAW_DIR

class TestDownloadSets(unittest.TestCase):

# Test 1: Simula respuesta exitosa de la función download_nadac y guardado localmente
    @patch("scripts.a_download_sets.requests.get")
    def test_download_nadac_success(self, mock_get):
        # Simula una respuesta con status code de 200 y un archivo csv dummy devuelto del request
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b"dummy CSV content"

        result = download_nadac()
        # Prueba que la función sí devuelve una respuesta
        self.assertIsNotNone(result)
        # Prueba que se guarda el archivo csv dummy localmente
        self.assertTrue(os.path.exists(result))

        # Limpieza
        os.remove(result)

# Test 2: Simula respuesta no exitosa de la función download_nadac y que la conexión falló
    @patch("scripts.a_download_sets.requests.get")
    def test_download_nadac_failure(self, mock_get):
        # Simula un connection error por parte del request
        mock_get.side_effect = ConnectionError("Falla de conexión")

        result = download_nadac()
        # Como la conexión falló, debe devolver None
        self.assertIsNone(result)

# Test 3: Simula respuesta exitosa de la función download_ndc pero para el archivo .zip y guardado localmente
    @patch("scripts.a_download_sets.requests.get")
    def test_download_ndc_success(self, mock_get):
        # Crea un archivo ZIP en memoria con un archivo JSON ficticio llamado drug_ndc.json
        json_content = b'{"mock_key": "mock_value"}'
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr("drug_ndc.json", json_content)
        zip_buffer.seek(0)

        mock_get.return_value.status_code = 200
        mock_get.return_value.content = zip_buffer.read()

        result = download_ndc()
        # Prueba que la función sí devuelve una respuesta
        self.assertIsNotNone(result)
        # Prueba que se guarda el archivo csv dummy localmente
        self.assertTrue(os.path.exists(result))

        # Limpieza del resultado
        os.remove(result)

# Test 4: Simula respuesta no exitosa de la función download_ndc y que la conexión falló
    @patch("scripts.a_download_sets.requests.get")
    def test_download_ndc_failure(self, mock_get):
        # Simula un connection error por parte del request
        mock_get.side_effect = ConnectionError("Falla de conexión")

        result = download_ndc()
        # Como la conexión falló, debe devolver None
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()


# correr script de pytest desde la terminal con:
# python -m unittest tests/test_download_sets.py