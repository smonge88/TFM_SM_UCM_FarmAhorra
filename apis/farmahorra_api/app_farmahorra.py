import os, json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import httpx
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from motor.motor_asyncio import AsyncIOMotorClient

"""
API de FarmAhorra – Orquestador de pedidos y catálogos

Este microservicio implementa la API central de FarmAhorra, encargada de
orquestar pedidos hacia múltiples farmacias simuladas y consolidar sus catálogos.

Características principales:
- Configuración dinámica de farmacias vía variable de entorno `PHARMACIES` 
  (JSON que mapea IDs de farmacia a sus URLs base).
- Conexión a CosmosDB (Mongo API) o MongoDB local para almacenar pedidos
  confirmados en la colección `orders_farmahorra`.
- Uso de FastAPI para definir endpoints REST de orquestación.

Endpoints principales:
- `POST /orders`: recibe un pedido, lo enruta a la farmacia correspondiente,
  valida stock, descuenta cantidades y guarda la orden en MongoDB.
- `GET /orders/{external_order_id}`: devuelve un pedido específico desde la base central.
- `GET /orders`: lista pedidos confirmados con filtros por `id_farmacia` o `client_id`.
- `GET /products`: combina los catálogos de todas las farmacias registradas
  (o de una específica), soportando paginación y filtro por stock.

Notas técnicas:
- Se usa `httpx.AsyncClient` para llamadas asíncronas a las APIs de farmacias.
- Se emplea `motor` (`AsyncIOMotorClient`) para interacción asíncrona con MongoDB.
- Las órdenes generadas y respuestas aseguran validación de datos,
  incluyendo NDC-11, cantidades mínimas y cálculo de totales.
- Todos los timestamps se manejan en UTC y se normalizan al formato `datetime`.
"""

# ---------- Config mínima ----------
# Un solo ENV en formato JSON para mapear farmacias:
PHARMACIES = json.loads(os.getenv("PHARMACIES",
                                  '{"farma_001":"http://localhost:8001"},'
                                  '{"farma_002":"http://localhost:8002"},'
                                  '{"farma_003":"http://localhost:8003"}'))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "fa_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "orders_farmahorra")

app = FastAPI(
    title="FarmAhorra",
    description=(
        "Orquestador de pedidos: recibe la orden, llama a la farmacia "
        "para validar/ejecutar y guarda el resultado en Cosmos (Mongo API)."
    ),
    version="1.0.0",
)

# Cliente asíncrono hacia Cosmos (Mongo API).
mongo = AsyncIOMotorClient(MONGO_URI)
orders = mongo[MONGO_DB][MONGO_COLLECTION]

# ----------- Definición de clases -----------
class ItemIn(BaseModel):
    """Clase para un Item de una orden"""
    package_ndc_11: str
    quantity: int = Field(ge=1)

class CreateOrder(BaseModel):
    """
    Clase que define los elementos de una orden
    - id_farmacia: destino donde se valida stock y se descuenta
    - external_order_id: identificador propio de FarmAhorra
    """
    id_farmacia: str
    external_order_id: str
    client_id: Optional[str] = None
    discount_pct: float = 0
    items: List[ItemIn]

class ItemOut(ItemIn):
    """Item de salida con precio unitario, total y descripciones."""
    unit_price: float
    line_total: float
    descripcion: Optional[str] = None
    generic_name: Optional[str] = None

class OrderOut(BaseModel):
    """
    Respuesta final de la orden completada, con elementos agregados a la orden.
    """
    external_order_id: str
    farmacia_order_id: str
    id_farmacia: str
    client_id: Optional[str] = None
    discount_pct: float = 0
    items: List[ItemOut]
    subtotal: float
    discount: Optional[float] = None
    total: float
    confirmed_at: datetime


# ---------- Endpoint post ----------
@app.post("/orders", response_model=OrderOut, status_code=201)
async def create_order(payload: CreateOrder):
    """
    Para crear un pedido:

    1) Resuelve la URL de la farmacia según `id_farmacia`.
    2) Llama a POST /orders de la farmacia con el body original (sin id_farmacia).
        - La farmacia valida stock y descuenta.
        - Si todo está bien, devuelve 201 con precios y totales.
    3) Persiste en Cosmos (orders_farmahorra) y retorna el documento.

    - Si la farmacia responde con un HTTP != 201, se propaga el error directamente.
    """
    # Valida que la farmacia exista
    base_url = PHARMACIES.get(payload.id_farmacia)
    if not base_url:
        raise HTTPException(404, f"Farmacia desconocida: {payload.id_farmacia}")

    # Construye el body que necesita la farmacia (sin id_farmacia)
    body = {
        "items": [i.model_dump() for i in payload.items],
        "external_order_id": payload.external_order_id,
        "client_id": payload.client_id,
        "discount_pct": payload.discount_pct,
    }

    # Genera llamada a la farmacia
    async with httpx.AsyncClient(timeout=15) as hc:
        r = await hc.post(f"{base_url}/orders", json=body)

    # Genera error en caso de que la farmacia no responda
    if r.status_code != 201:
        raise HTTPException(r.status_code, r.text)

    pharm = r.json()

    # Define timestamp para confirmed_at, si falla el parse, usa now()
    try:
        dt = datetime.fromisoformat(str(pharm.get("confirmed_at", "")).replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)

    # Documento final a devolver. Algunos campos provienen de la respuesta de orden devuelta por la farmacia
    doc = {
        "external_order_id": payload.external_order_id,
        "farmacia_order_id": pharm["order_id"],
        "id_farmacia": payload.id_farmacia,
        "client_id": payload.client_id,
        "discount_pct": pharm.get("discount_pct", payload.discount_pct),
        "items": pharm.get("items", []),
        "subtotal": pharm.get("subtotal"),
        "discount": pharm.get("discount"),
        "total": pharm.get("total"),
        "confirmed_at": dt.isoformat(),  # guardado como ISO string
        "source": "farmahorra",
    }

    await orders.insert_one(doc)

    # Para cumplir el response_model se convierte de ISO a datetime
    doc["confirmed_at"] = dt
    return doc

# ---------- Endpoint get order ----------
@app.get("/orders/{external_order_id}", response_model=OrderOut)
async def get_order(external_order_id: str):
    """
    Permite obtener un pedido por `external_order_id`.

    - Busca el documento en Cosmos (Mongo API).
    - Oculta `_id`.
    - Convierte `confirmed_at` a datetime si viniera como string.
    """
    # Utiliza '_id' de Mongo de la API
    doc = await orders.find_one({"external_order_id": external_order_id}, {"_id": 0})
    if not doc:
        raise HTTPException(404, "No existe el pedido")

    # Asegura datetime en la respuesta
    for k in ("confirmed_at"):
        v = doc.get(k)
        if isinstance(v, str):
            try:
                doc[k] = datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                doc[k] = datetime.now(timezone.utc)

    return doc

# ---------- Endpoint get orders ----------
@app.get("/orders", response_model=list[OrderOut])
async def list_orders(
    limit: int = 50,            # cambia el default si querés más
    offset: int = 0,            # paginación simple
    id_farmacia: Optional[str] = None,
    client_id: Optional[str] = None,
):
    """
    Enlista pedidos confirmados en JSON (ordenados por confirmed_at desc).
    Filtros opcionales por id_farmacia y client_id.
    """
    filt: dict = {}
    if id_farmacia:
        filt["id_farmacia"] = id_farmacia
    if client_id:
        filt["client_id"] = client_id

    cursor = (
        orders.find(filt, {"_id": 0})
        .sort("confirmed_at", -1)
        .skip(offset)
        .limit(limit)
    )

    out: list[dict] = []
    async for doc in cursor:
        # Normaliza timestamps a datetime (el response_model lo exige)
        v = doc.get("confirmed_at")
        if isinstance(v, str):
            try:
                doc["confirmed_at"] = datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                doc["confirmed_at"] = datetime.now(timezone.utc)
        out.append(doc)
    return out

# ---------- Endpoint get products ----------
@app.get("/products")
async def list_products(
    id_farmacia: Optional[str] = None,
    in_stock_only: bool = False,
    limit: int = 200,
    offset: int = 0,
):
    """
    Agrega los catálogos de todas las farmacias y devuelve un solo JSON.
    - Mantiene el formato de cada farmacia.
    - Solo añade 'id_farmacia' para indicar el origen.
    - Llama a /catalog/products de cada farmacia en paralelo.
    - Paginación simple con limit/offset sobre la lista combinada.
    """

    # Elige farmacias a consultar
    farmacia_ids: List[str] = (
        [id_farmacia] if id_farmacia else list(PHARMACIES.keys())
    )

    # Prepara llamadas concurrentes
    async def fetch_catalog(fid: str, url: str) -> List[Dict[str, Any]]:
        try:
            async with httpx.AsyncClient(timeout=15) as hc:
                r = await hc.get(f"{url}/catalog/products")
            if r.status_code != 200:
                return []
            items = r.json()
            # Asegura lista
            if not isinstance(items, list):
                return []
            # Añade id_farmacia a cada item sin tocar el resto de campos
            for it in items:
                # solo agrega id_farmacia si no existe
                if isinstance(it, dict) and "id_farmacia" not in it:
                    it["id_farmacia"] = fid
            # Filtro por stock si se pide
            if in_stock_only:
                items = [it for it in items if isinstance(it, dict) and it.get("stock", 0) > 0]
            return items
        except Exception:
            # Si una farmacia está caída o tarda demasiado, se omite
            return []

    tasks = [fetch_catalog(fid, PHARMACIES[fid]) for fid in farmacia_ids if fid in PHARMACIES]
    results = await asyncio.gather(*tasks)

    # Combina y pagina
    combined: List[Dict[str, Any]] = []
    for lst in results:
        combined.extend(lst)

    # Paginación global sobre la lista combinada
    total = len(combined)
    sliced = combined[offset : offset + limit]

    # Incluyo algunos metadatos al inicio del catálogo
    return {
        "count": len(sliced),
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": sliced,
    }

