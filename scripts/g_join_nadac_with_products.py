import pandas as pd
from pymongo import MongoClient
import os

#-------------------------------------------------------------------------------------------------------
# Este script tiene como objetivo crear un solo dataset cruzando la información de nadac_clean.csv con
# "products_with_selling_size" de la base de datos ndc_db de MongoDB.
# Se trata de un inner join para mantener en lo posible todos los productos de nadac_clean que tienen el dato de
# costo por unidad según el producto y ubicar la información complementaria del archivo json que contiene información
# del producto, así como la cantidad de unidades, mililitros o gramos por producto empacado. Aquellos productos que
# estén en "products_with_selling_size" pero no nadac_clean.csv se descartan. Igualmente aquellos productos que estén
# en nadac_clean.csv pero no en "products_with_selling_size" se descartan.
# Luego de hacer el cruce, se hace un cálculo para obtener el "estimated_total_price" a partir de multiplicar el NADAC por
# "selling_size".
# Finalmente para entregar el dataset resultante, se hace un filtrado de las columnas de interés, así como un aplanado
# de los atributos que estaban inmersos en otros, para obtener un archivo plano csv con todos los datos a procesar
# en los siguientes pasos.
# Como input se tiene el archivo "products_with_selling_size" y como output se exporta "products_enriched" a MongoDB también
# y se guarda localmente en el directorio del proyecto data/output el archivo "products_enriched.csv".
#-------------------------------------------------------------------------------------------------------

# 1. Me conecto a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ndc_db"]
products_collection = db["products_with_selling_size"]
enriched_collection = db["products_enriched"]

# 2. Leo dataset de productos desde MongoDB y lo convierto en una lista
print("Lectura de productos desde MongoDB")
products_cursor = products_collection.find()
products = list(products_cursor)

# 3. Expando packaging para tener una fila por package_ndc_11
print("Expansión de packaging")
rows = []
for doc in products:
    product_ndc = doc.get("product_ndc")
    for pkg in doc.get("packaging", []):
        rows.append({
            "product_ndc": product_ndc,
            "package_ndc_11": pkg.get("package_ndc_11"),
            "description": pkg.get("description"),
            "selling_size": pkg.get("selling_size"),
            "full_product": doc  # se guarda el documento completo para conservar el resto de info útil
        })
# Obtengo un dataframe que luego puedo cruzar con nadac_clean.csv
products_df = pd.DataFrame(rows)

# 4. Leo nadac_clean.csv
print("Carga de archivo nadac_clean.csv")
nadac_path = os.path.join("data", "cleaned", "nadac_clean.csv")
nadac_df = pd.read_csv(nadac_path, dtype={"NDC": str})

# 5. Hago un INNER JOIN: NDC con package_ndc_11, para cruzar todos los productos existentes en el csv de nadac con el json preprocesado de products_ndc
print("Aplicación del join por NDC")
merged_df = nadac_df.merge(
    products_df,
    left_on="NDC",
    right_on="package_ndc_11",
    how="inner"
)

# 6. Reporte de cuántos hicieron match y cuántos no
matched_count = merged_df["package_ndc_11"].notna().sum()
unmatched_count = merged_df["package_ndc_11"].isna().sum()
print(f"Registros con match de NDC: {matched_count}")
print(f"Registros sin match de NDC: {unmatched_count}")

# 7. Filtro solo los que hicieron match
merged_df_nulos = merged_df.copy()
merged_df = merged_df[merged_df["product_ndc"].notna()].copy()

# 8. Calculo el precio total estimado por producto empacado.
# Aplico un redondeo y que multiplique por 10 aquellos costos menores a 1, como factor de correción.
merged_df["estimated_total_price"] = merged_df["NADAC Per Unit"] * merged_df["selling_size"]
merged_df["estimated_total_price"] = merged_df["estimated_total_price"].apply(
    lambda x: round(x * 10, 2) if x < 1 else round(x, 2)
)

# 9. Aplana campos de full_product para obtenerlos en una sola línea
# y no como un subset dentro del documento principal
merged_df["generic_name"] = merged_df["full_product"].apply(lambda d: d.get("generic_name"))
merged_df["labeler_name"] = merged_df["full_product"].apply(lambda d: d.get("labeler_name"))
merged_df["active_ingredients_name"] = merged_df["full_product"].apply(
    lambda d: d.get("active_ingredients", [{}])[0].get("name") if d.get("active_ingredients") else None
)
merged_df["active_ingredients_strength"] = merged_df["full_product"].apply(
    lambda d: d.get("active_ingredients", [{}])[0].get("strength") if d.get("active_ingredients") else None
)
merged_df["openfda_manufacturer_name"] = merged_df["full_product"].apply(
    lambda d: d.get("openfda", {}).get("manufacturer_name", [None])[0]
)
merged_df["dosage_form"] = merged_df["full_product"].apply(lambda d: d.get("dosage_form"))
merged_df["product_type"] = merged_df["full_product"].apply(lambda d: d.get("product_type"))
merged_df["pharm_class"] = merged_df["full_product"].apply(
    lambda d: "; ".join(d.get("openfda", {}).get("pharm_class_epc", []))
)

# 10. Selección de columnas
columns_to_keep = [
    "NDC Description", "NDC", "NADAC Per Unit", "Pricing Unit", "As of Date",
    "product_ndc", "package_ndc_11", "description", "selling_size", "estimated_total_price",
    "generic_name", "labeler_name", "active_ingredients_name", "active_ingredients_strength",
    "openfda_manufacturer_name", "dosage_form", "product_type", "pharm_class"
]
final_df = merged_df[columns_to_keep].dropna(subset=["NDC", "product_ndc", "estimated_total_price"])

# 11. Convierto a dictionary (JSON) para que sea legible por MongoDB
print("Guardando resultados en MongoDB en products_enriched")
records = final_df.to_dict(orient="records")

# Limpio colección anterior
enriched_collection.delete_many({})
enriched_collection.insert_many(records)

print(f"Total de documentos guardados: {len(records)}")

# 12. Guarda como CSV sin la columna '_id' si existe
print("Exportando colección enriquecida a CSV...")
export_dir = os.path.join("data", "output")
os.makedirs(export_dir, exist_ok=True)
export_path = os.path.join(export_dir, "products_enriched.csv")

# No exporta la columna id default de MongoDB
if "_id" in final_df.columns:
    final_df.drop(columns=["_id"], inplace=True)

final_df.to_csv(export_path, index=False)
print(f"Archivo exportado a: {export_path}")


# correr script desde terminal con
# python scripts/g_join_nadac_with_products.py
