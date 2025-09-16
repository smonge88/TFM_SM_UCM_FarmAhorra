# Copiar y pegar y ejecutar en mongosh CLI

# Asegura que los índices de las colecciones catalog_farma_001, catalog_farma_002,
# catalog_farma_003 queden correctos y compatibles con CosmosDB después de un mongorestore

# Revisa si existe un índice de texto con versión distinta a la 2 (textIndexVersion !== 2) y lo elimina.
# Crea el índice único por NDC en package_ndc_11.
# Crea el índice en updated_at.
# Crea el índice de texto descripcion y generic_name con versión 2.
# Se ejecuta en mongosh CLI una vez que los catálogos por farmacia son generados.

use ndc_db

const tenants = ["farma_001","farma_002","farma_003"];

for (const t of tenants) {
  const coll = db[`catalog_${t}`];

  try {
    const ix = coll.getIndexes().find(i => i.name === "text_desc_generic");
    if (ix && ix.textIndexVersion !== 2) {
      coll.dropIndex("text_desc_generic");
    }
  } catch (e) {}

  // único por NDC (clave para la API)
  try { coll.createIndex({ package_ndc_11: 1 }, { name: "uid_ndc", unique: true }); } catch(e) {}

  // updated_at usado para el filtro incremental `since`
  try { coll.createIndex({ updated_at: 1 }, { name: "idx_updated_at" }); } catch(e) {}

  // índice de texto compatible con Cosmos (v2)
  coll.createIndex(
    { descripcion: "text", generic_name: "text" },
    { name: "text_desc_generic", textIndexVersion: 2, default_language: "spanish" }
  );

  print("índices listos para", `catalog_${t}`);
}