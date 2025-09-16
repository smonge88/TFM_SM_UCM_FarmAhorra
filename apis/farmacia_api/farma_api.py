import os, re
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo import MongoClient, ASCENDING, TEXT
from routes_orders import build_orders_router

"""
API Catálogo de Farmacia

Este script implementa un microservicio con FastAPI que expone el catálogo de una farmacia simulada. 
Cada instancia se configura dinámicamente a partir de variables de entorno (`ID_FARMACIA`, `MONGO_URI`, `DB_NAME`) 
y se conecta a la colección correspondiente en MongoDB (`catalog_<ID_FARMACIA>`).

Características principales:
- Define un modelo `Product` con validaciones (usando Pydantic) para representar los productos de la farmacia.
- Expone endpoints REST para:
  * `/catalog/products`: lista productos del catálogo con opciones `query` y filtrado incremental mediante `since`.
  * `/catalog/products/{package_ndc_11}`: obtiene un producto específico por su código NDC de 11 dígitos.
- Incluye el router de órdenes (`build_orders_router`) para manejar operaciones de pedidos en la misma API.
- Configura índices en MongoDB para:
  * Búsqueda de texto en `descripcion` y `generic_name`.
  * Consultas incrementales por `updated_at`.
  * Unicidad por `package_ndc_11`.

Este microservicio forma parte de la simulación de farmacias dentro del proyecto, 
permitiendo exponer catálogos de forma independiente por cada farmacia simulada.
"""

# Lee ID de la farmacia desde variable de entorno
ID_FARMACIA = os.getenv("ID_FARMACIA")
if not ID_FARMACIA:
    raise ValueError("La variable de entorno 'ID_FARMACIA' no está definida.")

# Configuración del FastAPI
app = FastAPI(
    title=f"API Catálogo - {ID_FARMACIA}",
    description="Microservicio que expone el catálogo de una farmacia simulada.",
    version="1.0.0"
)

# Conexión con MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
DB_NAME = os.getenv("DB_NAME", "ndc_db")
db = client[DB_NAME]
COLLECTION_NAME = f"catalog_{ID_FARMACIA}"
coll = db[COLLECTION_NAME]

# Creación de índices para búsquedas o filtros
try:
    coll.create_index([("descripcion", TEXT), ("generic_name", TEXT)], name="text_desc_generic")
except Exception:
    pass
coll.create_index([("updated_at", ASCENDING)], name="idx_updated_at")
coll.create_index("package_ndc_11", name="uid_ndc", unique=True)

# Validación de la clase Product con los atributos por producto que debe devolver
class Product(BaseModel):
    package_ndc_11: str = Field(pattern=r"^\d{11}$", description="NDC con 11 dígitos sin guiones")
    descripcion: str
    price: float
    generic_name: Optional[str] = None
    selling_size: Optional[float] = None
    stock: Optional[int] = None
    manufacturer_name: Optional[str] = None
    updated_at: Optional[datetime] = None

# Monta el router de órdenes (usa la misma colección del catálogo y el tenant actual)
app.include_router(build_orders_router(coll,db, ID_FARMACIA))

# --- Definición de Endpoints por farmacia ---
# Endpoint para enlistar productos y hacer queries por palabra clave o una fecha de actualización del dataset
@app.get("/catalog/products", response_model=List[Product], summary="Listar productos")
def list_products(
    query: Optional[str] = Query(None, description="Búsqueda en descripcion/generic_name/NDC"),
    since: Optional[datetime] = Query(None, description="updated_at >= since"),
):
    mongo_filter = {}
    if since:
        mongo_filter["updated_at"] = {"$gte": since}

    projection = {"_id": 0}

    if query:
        esc = re.escape(query)
        # 1) Intento con $text (sin textScore)
        try:
            cursor = coll.find({**mongo_filter, "$text": {"$search": query}}, projection)
        except OperationFailure:
            # 2) Fallback a regex si $text no está disponible, en CosmosDB daba error por no encontrar los indices
            mongo_filter.pop("$text", None)
            or_regex = [
                {"descripcion": {"$regex": esc, "$options": "i"}},
                {"generic_name": {"$regex": esc, "$options": "i"}},
                {"package_ndc_11": {"$regex": esc}},  # NDC numérico
            ]
            cursor = coll.find({**mongo_filter, "$or": or_regex}, projection)
    else:
        cursor = coll.find(mongo_filter, projection)

    return list(cursor)

# Endpoint para obtener un producto a partir de un package_ndc específico
@app.get("/catalog/products/{package_ndc_11}", response_model=Product, summary="Obtener producto por NDC")
def get_product(package_ndc_11: str):
    if not re.fullmatch(r"\d{11}", package_ndc_11):
        raise HTTPException(status_code=400, detail={"code": "BAD_REQUEST", "message": "package_ndc_11 must match ^\\d{11}$"})

    doc = coll.find_one({"package_ndc_11": package_ndc_11}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail={"code": "NOT_FOUND", "message": "product not found"})
    return doc


# Endpoints por probar:

# http://localhost:8001/farma_001/catalog/products
# http://localhost:8002/farma_003/catalog/products
# http://localhost:8003/farma_003/catalog/products

# http://localhost:8001/docs
# http://localhost:8002/docs
# http://localhost:8003/docs

# Para borrar tanto imagenes como contenedores: docker-compose down --rmi all --volumes
# Para construir imagenes y contenedores: docker-compose up --build

# Para construir imagen farma_api.py ubicarme en apis/farmacia_api
# ejecutar docker build -t mongesam/farma_api:1.0.0 .
# exportar a docker hub con docker push mongesam/farma_api:1.0.0