import json
import random
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING, UpdateOne
from pathlib import Path

#-------------------------------------------------------------------------------------------------------
# Este script que cuenta con varias funciones, tiene como objetivo alimentarse del cátalogo base conocido
# como products_enriched que se encuentra en MongoDB.
# Mediante un procesamiento sencillo se desarrollan tres catálogos hijos (uno por farmacia ficticia),
# en el cual a cada uno se le asignó una cantidad aleatoria de productos, se determinó un precio de venta basado en
# "estimated_total_price" más un margen aleatorio y un stock de producto aleatorio también.
# Se escogieron cuáles atributos del catálogo original llevar a los catálogos por farmacia. También se incluyó un
# id_farmacia para diferenciar catálogo y archivo json de cuál farmacia se trata.
# Además también se hace un upsert de los productos por SDKU y updated_at,
# indexados para control posterior a la hora de hacer actualizaciones.
# Los catálogos resultantes se guardan tanto localmente como en la base de datos de MongoDB ya existente.
#-------------------------------------------------------------------------------------------------------

# Defino semilla fija para reproducibilidad en pruebas
random.seed(42)

# --------- CONFIGURACIÓN DE RUTAS Y CONEXIONES ---------

# Conexión al servidor local de MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ndc_db"]
collection = db["products_enriched"]

# Definición de rutas base
BASE_DIR = Path(__file__).resolve().parent.parent
FARMACIAS_PATH = BASE_DIR / "data" / "reference" / "farmacias.json"
OUTPUT_DIR = BASE_DIR / "data" / "catalogs"

# Crea carpeta de salida llamada catalogs si no existe
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Campos seleccionados de cada producto que se incluirán en el catálogo final
FIELDS_TO_INCLUDE = [
    "package_ndc_11",
    "NDC Description",
    "selling_size",
    "estimated_total_price",
    "generic_name",
    "openfda_manufacturer_name"
]

# Cantidad de productos por catálogo según el ID de farmacia. Definidos manualmente a gusto.
FARMACIA_PRODUCT_COUNTS = {
    "farma_001": 10000,
    "farma_002": 13000,
    "farma_003": 8000
}


# --------- FUNCIONES AUXILIARES ---------

def load_farmacias(path):
    """
    Carga la lista de farmacias simuladas desde un archivo JSON.
    Cada farmacia tiene información como ID, nombre, ubicación y puerto local API.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_random_products(n):
    """
    Selecciona aleatoriamente 'n' productos desde MongoDB usando $sample,
    limitando la extracción a solo los campos necesarios.
    """
    total_docs = collection.estimated_document_count()

    if n >= total_docs:
        # Si se piden más productos de los disponibles, se devuelven todos
        return list(collection.find({}, {field: 1 for field in FIELDS_TO_INCLUDE}))

    # Muestra aleatoria directa desde MongoDB
    ids_cursor = collection.aggregate([
        {"$sample": {"size": n}}, # selecciona n documentos aleatorios sin cargar toda la colección.
        {"$project": {field: 1 for field in FIELDS_TO_INCLUDE}}
        # devuelve solo los campos definidos en FIELDS_TO_INCLUDE para evitar traer datos innecesarios
    ])
    return list(ids_cursor)


def generar_catalogo_farmacia(base_products, farmacia):
    """
    Genera un catálogo simulado para una farmacia específica.
    A cada producto se le asigna:
    - Un precio de venta con margen aleatorio sobre el precio base
    - Un stock aleatorio entre 0 y 50
    - El ID de la farmacia correspondiente
    """
    margen = random.uniform(0.05, 0.25)  # Margen entre 5% y 25%
    catalogo = []

    for product in base_products:
        try:
            base_price = float(product.get("estimated_total_price", 0))
            # Si no existe estimated_total_price, devuelve a 0 flotante
        except (ValueError, TypeError):
            base_price = 0.0
            # Se refiere a casos inválidos de estimated_total_price donde igual se guarda como 0 flotante.

        # Asigna el precio de venta por producto más margen
        precio_venta = round(base_price * (1 + margen), 2)
        # Asigna el stock por producto de forma aleatoria.
        stock = random.randint(0, 50)

        # Asigna información de catálogo base al nuevo catálogo por farmacia
        catalogo.append({
            "package_ndc_11": product.get("package_ndc_11"),
            "descripcion": product.get("NDC Description"),
            "generic_name": product.get("generic_name"),
            "selling_size": product.get("selling_size"),
            "price": precio_venta,
            "stock": stock,
            "manufacturer_name": product.get("openfda_manufacturer_name"),
            "id_farmacia": farmacia["id_farmacia"]
        })

    return catalogo


def guardar_catalogo(catalogo, farmacia_id):
    """
    Guarda el catálogo generado como un archivo JSON en la carpeta de salida.
    El nombre del archivo incluye el ID de la farmacia.
    """
    output_path = OUTPUT_DIR / f"catalog_{farmacia_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(catalogo, f, indent=2, ensure_ascii=False)

    print(f"Catálogo guardado: {output_path.name} ({len(catalogo)} productos)")


def guardar_en_mongodb(catalogo, farmacia_id):
    """
    Inserta o actualiza documentos en MongoDB a nivel de catálogo de farmacia.

    Comportamiento:
    - Índice único en `package_ndc_11` para evitar duplicados (fail-fast).
    - Índice en `updated_at` para consultas incrementales (ej. `since`).
    - Índice de texto sobre `descripcion` y `generic_name` para búsquedas textuales.
    - Cada documento recibe:
        * `updated_at`: siempre actualizado con la fecha actual.
        * `created_at`: asignado únicamente en la primera inserción.

    Parameters
    ----------
    catalogo : list[dict]
        Lista de documentos (productos) a insertar o actualizar en la colección.
    farmacia_id : str
        Identificador de la farmacia.

    Returns
    -------
    None
        La función no retorna ningún valor. Su efecto principal es actualizar MongoDB
        e imprimir un resumen de operaciones.
    """
    nombre_coleccion = f"catalog_{farmacia_id}"
    coll = db[nombre_coleccion]

    # Índice único por package_ndc_11: asegura que no existan duplicados en el catálogo.
    # updated_at: facilita consultas incrementales basadas en la última actualización.
    # Índice de texto: permite búsquedas en campos "descripcion" y "generic_name".
    coll.create_index("package_ndc_11", name="uid_ndc", unique=True)
    coll.create_index([("updated_at", ASCENDING)], name="idx_updated_at")
    coll.create_index(
        [("descripcion", "text"), ("generic_name", "text")],
        name="text_desc_generic",
        default_language="spanish",
        textIndexVersion=2
    )

    now = datetime.now(timezone.utc)

    # Cada UpdateOne actúa como un UPSERT:
    # Si encuentra el documento por package_ndc_11 -> aplica $set (actualiza datos + updated_at).
    # Si no lo encuentra -> lo inserta y además asigna created_at.
    ops = []
    for item in catalogo:
        ndc = item.get("package_ndc_11")
        if not ndc:
            continue
        ops.append(UpdateOne(
            {"package_ndc_11": ndc}, # Filtro de búsqueda
            {"$set": {**item, "updated_at": now},  # Actualiza datos + updated_at
                    "$setOnInsert": {"created_at": now}}, # Solo en inserción inicial
            upsert=True
        ))

    if ops:
        res = coll.bulk_write(ops, ordered=False)
        print(f"[{nombre_coleccion}] upserted={res.upserted_count} modified={res.modified_count} matched={res.matched_count}")
    else:
        print(f"[{nombre_coleccion}] sin operaciones")


# --------- FUNCIÓN PRINCIPAL ---------

if __name__ == "__main__":
    print("Generando catálogos de farmacias...")

    # Carga datos de farmacias desde archivo estático
    farmacias = load_farmacias(FARMACIAS_PATH)

    # Genera y guarda catálogo individual por cada farmacia
    for farmacia in farmacias:
        farmacia_id = farmacia["id_farmacia"]
        cantidad = FARMACIA_PRODUCT_COUNTS.get(farmacia_id, 10000)
        # Usa 10000 por default, en caso de que no esté definida la cantidad

        print(f"Generando catálogo para {farmacia['nombre_farmacia']} con {cantidad} productos...")

        # Genera lista de productos de tamaño n por farmacia
        productos_random = get_random_products(cantidad)
        # Genera el catálogo por farmacia a partir de la muestra de productos asignada y le asigna los campos de agregación
        catalogo = generar_catalogo_farmacia(productos_random, farmacia)
        # Guarda catálogo por farmacia como archivo json
        guardar_catalogo(catalogo, farmacia_id)
        # Guarda catálogo de cada farmacia también en MongoDB
        guardar_en_mongodb(catalogo, farmacia_id)

    print("\nTodos los catálogos fueron generados exitosamente")


# correr script desde terminal con
# python scripts/h_generate_catalogs.py
