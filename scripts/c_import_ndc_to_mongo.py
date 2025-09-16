import json
import os
from pymongo import MongoClient

#-------------------------------------------------------------------------------------------------------
# Este script exporta el archivo drug-ndc-0001-of-0001.json a MongoDB.
# Crea la base de datos ndc_db y la colección products que es de donde se insertan los productos del archivo
# json como documentos de la base de datos.
# El objetivo es visualizar, hacer queries rápidos y manipular con mayor facilidad los documentows en MongoDB
#-------------------------------------------------------------------------------------------------------


# Configuración de los paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_PATH = os.path.join(BASE_DIR, "data", "raw", "drug-ndc-0001-of-0001.json")

# Se conecta a MongoDB (contenedor mongo-TFM expuesto en localhost:27017)
client = MongoClient("mongodb://localhost:27017")
db = client["ndc_db"]
collection = db["products"]

def import_ndc_data():
    """
    Importa datos del archivo NDC en formato JSON a la colección de MongoDB.

    Pasos principales:
    1. Abre y carga el archivo JSON desde `JSON_PATH`.
    2. Accede a la clave `results` para obtener los documentos a insertar.
    3. Inserta los documentos en la colección `products` de MongoDB.
    4. Informa en consola la cantidad de documentos insertados.

    Returns
    -------
    None
        La función no retorna ningún valor. Su efecto principal es la inserción de datos en MongoDB.

    """
    print("Cargando archivo JSON...")
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Necesito acceder a la clave results dentro del diccionario de data
    documents = data.get("results", [])
    print(f"Total de documentos a insertar: {len(documents)}")

    if documents:
        # Importa el archivo json a la coleccion en  mongo
        result = collection.insert_many(documents)
        print(f"Se insertaron {len(result.inserted_ids)} documentos en la colección 'products'.")
    else:
        print("No se encontraron documentos en la clave 'results'.")

if __name__ == "__main__":
    import_ndc_data()

# correr script desde terminal con
# python scripts/c_import_ndc_to_mongo.py