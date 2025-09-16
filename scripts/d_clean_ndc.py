from pymongo import MongoClient

#-------------------------------------------------------------------------------------------------------
# Este script lee los documentos del dataset products y escribe - y exporta - un dataset llamado products_cleaned
# La limpieza se basa en filtrar y eliminar aquellos documentos (productos) que no tienen product_ndc,
# y/o que el product_ndc no sigue el patrón numérico correcto. Asimismo, mantiene los documentos que tengan un
# packaging, que el package_ndc exista, así como la descripción.
# Por último, comprueba que la descripción comience por un número int o float.
# Da un reporte de la cantidad de productos filtrados y eliminados.
# La limpieza se hace siguiendo el lenguaje de agregaciones de MongoDB.
#-------------------------------------------------------------------------------------------------------


# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ndc_db"]

# Colecciones
source_collection = db["products"]
cleaned_collection = db["products_cleaned"]

# Conteo documentos originales antes del pipeline
original_count = source_collection.count_documents({})
print(f"Total de documentos originales en 'products': {original_count}")

# Pipeline de limpieza tomado de las agregaciones de MongoDB
pipeline = [
    {
        "$match": {
            "product_ndc": {
                "$exists": True,
                "$type": "string",
                "$regex": "^(\d{5}-\d{3}|\d{4}-\d{4}|\d{5}-\d{4})$"
            }
        }
    },
    {
        "$addFields": {
            "packaging": {
                "$filter": {
                    "input": "$packaging",
                    "as": "pkg",
                    "cond": {
                        "$and": [
                            {"$ne": ["$$pkg.package_ndc", None]},
                            {"$ne": ["$$pkg.package_ndc", ""]},
                            {"$ne": ["$$pkg.description", None]},
                            {"$ne": ["$$pkg.description", ""]},
                            {
                                "$regexMatch": {
                                    "input": "$$pkg.description",
                                    "regex": "^(\\d+(\\.\\d+)?|\\.\\d+)"
                                }
                            }
                        ]
                    }
                }
            }
        }
    },
    # Asegura que haya al menos un packaging válido
    {"$match": {"packaging": {"$ne": []}}},

# Deduplica packaging por package_ndc, para evitar tener package_ndc duplicados
    {
        "$set": {
            "packaging": {
                "$reduce": {
                    "input": "$packaging",
                    "initialValue": {
                        "seen": [],      # lista de NDC ya vistos
                        "items": []      # array deduplicado que construiré
                    },
                    "in": {
                        "$cond": [
                            # Pregunta si el package_ndc actual ya está en 'seen'?
                            {"$in": ["$$this.package_ndc", "$$value.seen"]},
                            # Sí: devuelve el acumulador tal cual (omite el duplicado)
                            "$$value",
                            # No: agrego este item y registro su NDC en 'seen'
                            {
                                "seen": {
                                    "$concatArrays": ["$$value.seen", ["$$this.package_ndc"]]
                                },
                                "items": {
                                    "$concatArrays": ["$$value.items", ["$$this"]]
                                }
                            }
                        ]
                    }
                }
            }
        }
    },
    # Extraigo solo el array deduplicado (packaging.items)
    {"$set": {"packaging": "$packaging.items"}},

    # Filtro el array tras deduplicar
    {"$match": {"packaging": {"$ne": []}}},

    {
        "$merge": {
            "into": "products_cleaned",
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }
    }
]

# Ejecuta pipeline
source_collection.aggregate(pipeline)

# Cuenta documentos limpios y eliminados
cleaned_count = cleaned_collection.count_documents({})
removed_count = original_count - cleaned_count

# Muestra resumen
print(f"Documentos en 'products_cleaned': {cleaned_count}")
print(f"Documentos eliminados: {removed_count}")

# correr script desde terminal con
# python scripts/d_clean_ndc.py

