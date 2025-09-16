import re
from pymongo import MongoClient

#-------------------------------------------------------------------------------------------------------
# Este script extrae y guarda el número (entero o decimal) correspondiente a la cantidad de unidades,
# mililitros o gramos totales por producto empacado (package_ndc).
# Lee el dataset "products_normalized" de MongoDB
# Guarda la variable numérica dentro de packaging y crea un nuevo dataset llamado products_with_selling_size
# y lo exporta a MongoDB.
#-------------------------------------------------------------------------------------------------------

# Función para extraer el número (entero o float) con el que inicia el campo description
def extract_selling_size(description):
    """
        Extrae el valor numérico inicial de un campo de texto de descripción.

        Parameters
        ----------
        description : str
            Texto de descripción del producto. Se espera que pueda comenzar con un valor numérico.

        Returns
        -------
        float or None
            Número extraído al inicio de la descripción convertido a `float`.
            Devuelve `None` si la entrada no es cadena o no comienza con un número válido.

        Examples
        --------
        >>> extract_selling_size("10 mg tablet")
        10.0
        >>> extract_selling_size("0.5 ml vial")
        0.5
        >>> extract_selling_size(".75 g powder")
        0.75
        >>> extract_selling_size("Tablet without number")
        None
        >>> extract_selling_size(12345)  # entrada no válida
        None
    """
    if not isinstance(description, str):
        return None

    # Uso regex para capturar números decimales o enteros
    match = re.match(r"^(\d+(\.\d+)?|\.\d+)", description.strip())

    if match:
        try:
            return float(match.group())  # Convierte a float
        except ValueError:
            return None
    return None

# Función principal para recorrer los documentos y agregar el campo "selling_size"
def process_and_add_selling_size():
    """
        Flujo principal:
        1. Conecta a MongoDB local en la base de datos `ndc_db`.
        2. Lee los documentos de la colección `products_normalized`.
        3. Recorre cada documento y sus campos `packaging`.
        4. Aplica `extract_selling_size` para extraer números al inicio de `description`.
        5. Agrega el valor detectado como `selling_size` dentro de cada `packaging`.
        6. Inserta los documentos actualizados en la colección `products_with_selling_size` en la base `ndc_db`.
        7. Imprime un resumen con la cantidad de productos procesados y con `selling_size`.

        Returns
        -------
        None
            La función no retorna ningún valor. Su efecto principal es insertar documentos
            modificados en una colección de MongoDB.
    """
    # 1. Me conecto a la base de datos MongoDB local
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ndc_db"]

    # Colección de source (ya normalizada) y target (con selling_size)
    source = db["products_normalized"]
    target = db["products_with_selling_size"]

    # 2. Defino contadores
    source_docs = source.find()
    updated_docs = []  # Lista para guardar los documentos modificados
    total = 0          # Total de productos procesados

    # 3. Procesa cada documento de la colección original
    for doc in source_docs:
        total += 1
        packaging = doc.get("packaging", [])  # Lista de packaging
        new_packaging = []

        # Procesa cada item dentro de packaging
        for pkg in packaging:
            description = pkg.get("description", "")
            size = extract_selling_size(description) # Aplica funcion extract_selling_size sobre description

            if size is not None:
                # Si se detecta un número al inicio del description, lo guardo como selling_size dentro del elemento pkg en packaging
                pkg["selling_size"] = size

            # Guardo el packaging actualizado (con o sin selling_size)
            new_packaging.append(pkg)

        # Reemplazo el packaging con la versión modificada
        doc["packaging"] = new_packaging

        # Añado el documento modificado a la lista para guardar
        updated_docs.append(doc)

    # 4. Guardo los documentos resultantes en la nueva colección
    target.drop()  # Elimino la colección anterior si existe para evitar duplicados

    if updated_docs:
        result = target.insert_many(updated_docs)
        print(f"Documentos insertados en 'products_with_selling_size': {len(result.inserted_ids)}")
    else:
        print("No se insertó ningún documento.")

    # 5. Resumen final: cuántos productos tienen al menos un selling_size
    docs_with_size = db["products_with_selling_size"].count_documents(
        { "packaging.selling_size": { "$exists": True } }
    )

    print("\nResumen:")
    print(f"* Productos procesados: {total}")
    print(f"* Productos con al menos un selling_size: {docs_with_size}")
    print(f"* Guardados en MongoDB -> ndc_db.products_with_selling_size")

if __name__ == "__main__":
    process_and_add_selling_size()

# correr script desde terminal con
# python scripts/f_add_selling_size.py