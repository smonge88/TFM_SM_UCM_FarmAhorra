import re
from pymongo import MongoClient

#-------------------------------------------------------------------------------------------------------
# Este script hace una normalización de package_ndc a una variable string de 11 dígitos
# Por documento (producto) lee todos los package_ndc existentes, verifica que tenga un patrón numérico válido,
# agrega ceros según corresponda y elimina guiones con tal de obtener un string de 11 dígitos.
# Lee el dataset "products_cleaned" de MongoDB
# Crea una nueva variable por cada package_ndc existente y guarda este valor.
# Crea un nuevo dataset llamado products_normalized y lo exporta a MongoDB.
#-------------------------------------------------------------------------------------------------------

# Función auxiliar para normalizar el package_ndc a formato de 11 dígitos sin guiones
def normalize_package_ndc(package_ndc):
    """
    La función toma un código NDC en formato con guiones y agrega ceros
    dependiendo del patrón detectado, siguiendo los lineamientos de la FDA
    para convertir NDCs a 11 dígitos.
    Si el código no cumple con ninguno de los formatos reconocidos, devuelve None.

    Reglas de normalización:
    - Caso 1: Formato XXXX-XXXX-XX → agrega un 0 al inicio.
    - Caso 2: Formato XXXXX-XXX-XX → agrega un 0 después del primer guion.
    - Caso 3: Formato XXXXX-XXXX-X → agrega un 0 después del segundo guion.
    - Caso válido directo: XXXXX-XXXX-XX → no requiere cambios.
    - Cualquier otro formato no reconocido → devuelve None.

    Parameters
    ----------
    package_ndc : str
        Código NDC en formato con guiones.

    Returns
    -------
    str or None
        NDC normalizado como cadena de 11 dígitos sin guiones.
        Devuelve None si la entrada no es válida .
    """
    if not isinstance(package_ndc, str):
        return None

    # Caso 1: XXXX-XXXX-XX -> agrega 0 al inicio
    if re.fullmatch(r"\d{4}-\d{4}-\d{2}", package_ndc):
        package_ndc = "0" + package_ndc

    # Caso 2: XXXXX-XXX-XX -> agrega 0 después del primer guión
    elif re.fullmatch(r"\d{5}-\d{3}-\d{2}", package_ndc):
        parts = package_ndc.split("-")
        parts[1] = "0" + parts[1]
        package_ndc = "-".join(parts)

    # Caso 3: XXXXX-XXXX-X -> agregar 0 después del segundo guión
    elif re.fullmatch(r"\d{5}-\d{4}-\d", package_ndc):
        parts = package_ndc.split("-")
        parts[2] = "0" + parts[2]
        package_ndc = "-".join(parts)

    # Caso inválido (no cumple con ningún patrón reconocido)
    elif not re.fullmatch(r"\d{5}-\d{4}-\d{2}", package_ndc):
        return None

    # Elimina guiones y verifica que tenga 11 dígitos
    clean_ndc = package_ndc.replace("-", "")
    return clean_ndc if len(clean_ndc) == 11 else None

# Función principal que normaliza y guarda en Mongo
def process_and_normalize():
    """
        La función toma los documentos de la colección `products_cleaned`, aplica la función
        `normalize_package_ndc` a cada código `package_ndc` dentro del campo `packaging`,
        y si el resultado es válido, agrega un nuevo campo `package_ndc_11`. Los documentos
        que no contienen ningún `package_ndc` válido son descartados. Finalmente, los
        documentos normalizados se insertan en la colección `products_normalized` dentro de `ndc_db`.

        Returns
        -------
        None
            La función no retorna valores. Su efecto principal es la escritura de documentos
            en la colección de destino en MongoDB.
        """
    # 1. Me conecto a MongoDB local
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ndc_db"]

    # 2. Define la colección de origen y destino
    source = db["products_cleaned"]
    target = db["products_normalized"]

    # 3. Inicializa contadores y resultados
    source_docs = source.find()
    normalized_docs = []
    total_processed = 0
    total_discarded = 0

    # 4. Recorre todos los documentos de origen
    for doc in source_docs:
        total_processed += 1
        packaging = doc.get("packaging", [])
        new_packaging = []

        # 5. Recorre cada packaging del documento
        for pkg in packaging:
            pkg_ndc = pkg.get("package_ndc")
            normalized = normalize_package_ndc(pkg_ndc)

            # Si es válido, agrega nuevo campo package_ndc_11
            if normalized:
                pkg["package_ndc_11"] = normalized
                new_packaging.append(pkg)

        # 6. Guarda el producto si al menos uno de los package_ndc fue válido
        if  new_packaging:
            doc["packaging"] = new_packaging
            normalized_docs.append(doc)
        else:
            total_discarded += 1  # descarta si ninguno fue válido

    # 7. Guarda en MongoDB la colección de destino
    target.drop() # limpia la colección anterior si existe
    if normalized_docs:
        result = target.insert_many(normalized_docs)
        print(f"Documentos insertados en 'products_normalized': {len(result.inserted_ids)}")
    else:
        print("No se insertó ningún documento.")

    # 8. Imprimo resumen
    print(f"\n Resumen:")
    print(f"* Productos procesados: {total_processed}")
    print(f"* Productos descartados (por package_ndc inválido): {total_discarded}")
    print(f"* Productos insertados: {len(normalized_docs)}")

if __name__ == "__main__":
    process_and_normalize()


# correr script desde terminal con
# python scripts/e_normalize_package_ndc.py