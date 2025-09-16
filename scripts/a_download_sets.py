import requests
import zipfile
import io
import os

#-------------------------------------------------------------------------------------------------------
# Este script descarga desde los sitios web correspondientes, los archivos nadac.csv (con información de los
# productos y su respectivo costo unitario) y por otra parte al archivo drug-ndc.json que contiene información
# complementaria a los productos así como una descripción con el estimados de unidades o mililitros o gramos por
# producto. Los archivos se guardan en el directorio base de este proyecto y se crea la carpeta data/raw.
#-------------------------------------------------------------------------------------------------------

NADAC_URL = "https://download.medicaid.gov/data/nadac-national-average-drug-acquisition-cst07232025.csv"
NDC_ZIP_URL = "https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(DATA_RAW_DIR, exist_ok=True)


def download_nadac():
    """
        Descarga el archivo NADAC (National Average Drug Acquisition Cost) en formato CSV.

        El archivo se obtiene desde la URL definida en la constante `NADAC_URL` y se guarda
        en la carpeta especificada por `DATA_RAW_DIR` con el nombre `nadac.csv`.

        Returns
        -------
        str or None
            Ruta completa al archivo descargado en caso de éxito.
            None si ocurre un error durante la descarga.

        Raises
        ------
        requests.RequestException
            Si ocurre un error en la conexión HTTP (manejada dentro del try/except).
        """
    try:
        print("Descargando archivo NADAC...")
        response = requests.get(NADAC_URL, timeout=15)
        response.raise_for_status()
        path = os.path.join(DATA_RAW_DIR, "nadac.csv")
        with open(path, "wb") as f:
            f.write(response.content)
        print(f"NADAC guardado en: {path}")
        return path
    except requests.RequestException as e:
        print(f"Error al descargar NADAC: {e}")
        return None

def download_ndc():
    """
    Descarga y extrae el archivo NDC (National Drug Code) en formato JSON desde un ZIP.

    El archivo se obtiene desde la URL definida en la constante `NDC_ZIP_URL`.
    Luego, se descomprime el contenido, buscando el primer archivo con extensión `.json`,
    y se guarda en la carpeta especificada por `DATA_RAW_DIR`.

    Returns
    -------
    str or None
        Ruta completa al archivo JSON extraído en caso de éxito.
        None si ocurre un error durante la descarga o la extracción.

    Raises
    ------
    requests.RequestException
        Si ocurre errores en la conexión HTTP, si no es un ZIP válido
        o si nonencuentra ningún archivo json dentro del ZIP (manejada dentro del try/except).
    """
    try:
        print("Descargando archivo NDC (JSON ZIP)...")
        response = requests.get(NDC_ZIP_URL, timeout=15)
        response.raise_for_status()
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        json_name = [name for name in zip_file.namelist() if name.endswith(".json")][0]
        zip_file.extract(json_name, DATA_RAW_DIR)
        path = os.path.join(DATA_RAW_DIR, json_name)
        print(f"NDC JSON extraído en: {path}")
        return path
    except requests.RequestException as e:
        print(f"Error al descargar NDC: {e}")
        return None
    except zipfile.BadZipFile as e:
        print(f"Error al descomprimir archivo ZIP del NDC: {e}")
        return None
    except IndexError:
        print("No se encontró ningún archivo JSON dentro del ZIP del NDC.")
        return None

if __name__ == "__main__":
    nadac_file = download_nadac()
    ndc_file = download_ndc()

    if nadac_file and ndc_file:
        print("\nDescarga completada exitosamente.")
    else:
        print("\nHubo errores durante la descarga.")

# correr script desde terminal con
# python scripts/a_download_sets.py