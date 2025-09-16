from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field, conint
from typing import List, Optional
from datetime import datetime, timezone
from uuid import uuid4
from pymongo.collection import Collection
from pymongo.database import Database
import re

"""
Rutas de Órdenes para Farmacias (FastAPI)

Este módulo define los modelos y endpoints relacionados con la gestión de órdenes
de compra dentro del microservicio de farmacia. Se implementa como un `APIRouter`
que puede montarse en la API principal de cada farmacia simulada.

Características principales:
- Define modelos de datos con Pydantic:
  * `OrderItem`: línea de pedido con NDC-11 y cantidad.
  * `OrderCreate`: payload de creación de una orden (items, cliente, descuento).
  * `OrderItemOut`: línea enriquecida con precio, totales y descripciones.
  * `OrderOut`: respuesta de una orden confirmada con totales y metadatos.

- Función `build_orders_router(catalog_coll, db, tenant)`:
  Crea un router con endpoints REST para manejar pedidos, conectado a una colección
  de catálogo (`catalog_<farmacia>`) y persistiendo en una colección de órdenes
  (`orders_<farmacia>`). También asegura índices para búsquedas frecuentes.

- Endpoints expuestos:
  * `POST /orders`: crea un pedido confirmado, valida stock, descuenta cantidades,
    aplica descuentos y devuelve un snapshot de la orden.
  * `GET /orders`: lista pedidos confirmados, con filtros por fecha, cliente y paginación.
  * `GET /orders/{order_id}`: obtiene una orden específica por su UUID.

"""

# Regex para validar NDC-11: exactamente 11 dígitos, sin guiones.
NDC11_RE = re.compile(r"^\d{11}$")

# ----------- Definición de clases -----------
class OrderItem(BaseModel):
    """Clase para un Item de una orden"""
    package_ndc_11: str = Field(pattern=r"^\d{11}$", description="NDC con 11 dígitos")
    quantity: conint(ge=1)  # entero >= 1

class OrderCreate(BaseModel):
    """ Clase de creación de orden: Listado de items """
    items: List[OrderItem]
    external_order_id: Optional[str] = None  # referencia externa de FarmAhorra
    client_id: Optional[str] = None  # id del cliente
    discount_pct: Optional[float] = Field(default=0.0, ge=0.0,le=100.0)  # monto de descuento por compra en FarmAhorra

class OrderItemOut(OrderItem):
    """ Clase de la respuesta de un item dentro de una orden """
    unit_price: Optional[float] = None      # precio unitario snapshot
    line_total: Optional[float] = None      # unit_price * quantity
    descripcion: Optional[str] = None
    generic_name: Optional[str] = None

class OrderOut(BaseModel):
    """
    Respuesta de una orden confirmada.
    - order_id: ID generado por este servicio.
    - confirmed_at: timestamp de confirmación (UTC).
    - items: snapshot de las líneas que se confirmaron.
    """
    order_id: str
    confirmed_at: datetime
    items: List[OrderItemOut]
    subtotal: Optional[float] = None
    total: Optional[float] = None
    discount_pct: Optional[float] = None # %
    discount: Optional[float] = None # monto calculado
    external_order_id: Optional[str] = None
    client_id: Optional[str] = None
    # Ignora campos extra persistidos (tenant, etc.)
    model_config = {"extra": "ignore"}

# ----------- Creación del router -----------
def build_orders_router(catalog_coll: Collection, db: Database, tenant: str) -> APIRouter:
    """
    Crea y devuelve un APIRouter con los endpoints de órdenes para una farmacia.
    Recibe:
      - catalog_coll: colección de catálogo de una farmacia
      - db: instancia de base de datos donde también se guardan las órdenes
      - tenant: ID de la farmacia, se usa para nombrar la colección de órdenes

    Endpoints:
      - POST /orders: valida stock, descuenta, enriquece líneas con precio y totales.
      - GET  /orders: lista pedidos.
      - GET  /orders/{order_id}: muestra un pedido puntual.
    """
    # El API Router modulariza endpoints en FastAPI. Sirve como una forma de conectarse a la farma_api para procesar ordenes.
    router = APIRouter()

    # Colección de órdenes por tenant, e índices de apoyo.
    orders_coll = db[f"orders_{tenant}"]
    try:
        orders_coll.create_index("order_id", name="uid_order", unique=True) # búsqueda por ID
        orders_coll.create_index("confirmed_at", name="idx_confirmed_at") # listados por fecha
        orders_coll.create_index("external_order_id", name="idx_external_order_id") # listado por Order ID de FarmAhorra
        orders_coll.create_index("client_id", name="idx_client_id") # listado por cliente

    except Exception:
        # Si los índices ya existen, simplemente se ignora.
        pass

# ----------- Asignación de endpoint POST ORDER -----------
    @router.post("/orders", response_model=OrderOut, status_code=201, summary="Crear pedido (consume stock)")
    def create_order(payload: OrderCreate):
        """
        Crea una orden “confirmada” (no hay reservas previas).
        Para cada ítem:
          - Verifica que el NDC sea válido (11 dígitos).
          - Intenta descontar stock con filtro "stock >= quantity".
          - Si algo falla, se regresa a los rebajos previos y responde 4xx con un detalle.
        Si todos los rebajos funcionan:
          - Inserta la orden y responde 201 con el contenido de la orden.
        """
        # La orden debe tener al menos una línea
        if not payload.items:
            raise HTTPException(status_code=422, detail={"message": "Debe incluir al menos un item"})

        # Validación de NDCs
        for it in payload.items:
            if not NDC11_RE.fullmatch(it.package_ndc_11):
                raise HTTPException(status_code=422, detail={"message": f"NDC inválido: {it.package_ndc_11}"})

        # Zona horario UTC para timestamp de confirmed_at y para updated_at de productos
        now = datetime.now(timezone.utc)

        # Acumula rebajos ya aplicados para poder deshacerlos si un ítem dentro de la lista de Orders falla
        # en caso de que haya más de un ítem dentro de la orden
        # Estructura: [(ndc, qty), (ndc, qty), ...] en el orden aplicado
        decremented = []

        # 1) Trae info del catálogo (precio/metadatos) de todos los NDC solicitados
        ndcs = [it.package_ndc_11 for it in payload.items]
        prod_cursor = catalog_coll.find(
            {"package_ndc_11": {"$in": ndcs}},
            {"_id": 0, "package_ndc_11": 1, "price": 1, "descripcion": 1, "generic_name": 1, "stock": 1}
        )
        prod_map: Dict[str, dict] = {p["package_ndc_11"]: p for p in prod_cursor}

        missing = [ndc for ndc in ndcs if ndc not in prod_map]
        if missing:
            raise HTTPException(status_code=404,
                                detail={"message": f"Producto(s) no encontrado(s): {', '.join(missing)}"})

        try:
            # 2) Recorre ítems y para cada uno intenta restar al stock con un update atómico
            for it in payload.items:
                ndc = it.package_ndc_11
                qty = int(it.quantity)

                # El filtro "stock >= qty" evita que stock quede negativo.
                # Si no cumple, update_one no modifica nada (modified_count=0).
                res = catalog_coll.update_one(
                    {"package_ndc_11": ndc, "stock": {"$gte": qty}},
                    {"$inc": {"stock": -qty}, "$set": {"updated_at": now}}
                )

                if res.modified_count != 1:
                    # No se pudo rebajar: obtiene el stock actual para informar al cliente
                    doc = catalog_coll.find_one({"package_ndc_11": ndc}, {"_id": 0, "stock": 1})
                    available = int((doc or {}).get("stock") or 0)

                    # Rollback de los rebajos ya hechos (en orden inverso)
                    for done_ndc, done_qty in reversed(decremented):
                        catalog_coll.update_one(
                            {"package_ndc_11": done_ndc},
                            {"$inc": {"stock": done_qty}}
                        )

                    # Respuesta clara de conflicto de stock (409)
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "STOCK_CONFLICT",
                            "message": f"Stock insuficiente para {ndc}",
                            "shortage": {
                                "package_ndc_11": ndc,
                                "requested": qty,
                                "available": max(available, 0)
                            }
                        }
                    )

                # Rebajo correcto: dejo registro para posible rollback futuro
                decremented.append((ndc, qty))

                # 3) Agrega información de las líneas y calcula totales
                items_out: List[OrderItemOut] = []
                subtotal = 0.0
                for it in payload.items:
                    ndc = it.package_ndc_11
                    qty = int(it.quantity)
                    prod = prod_map[ndc]
                    unit_price = float(prod.get("price") or 0.0)
                    line_total = round(unit_price * qty, 2)
                    subtotal = round(subtotal + line_total, 2)
                    items_out.append(OrderItemOut(
                        package_ndc_11=ndc,
                        quantity=qty,
                        unit_price=unit_price,
                        line_total=line_total,
                        descripcion=prod.get("descripcion"),
                        generic_name=prod.get("generic_name"),
                    ))
                # 4) Descuento por porcentaje
                pct = float(payload.discount_pct or 0.0)
                discount_amount = round(subtotal * pct / 100.0, 2)
                total = round(subtotal - discount_amount, 2)

            # A este punto, todos los ítems fueron rebajados correctamente.
            # 5) Creo el documento de orden con un ID nuevo (UUID).
            order_doc = {
                "order_id": str(uuid4()), # formato hex (32 hex + 4 guiones = 36 chars)
                "confirmed_at": now,
                "items": [i.model_dump() for i in items_out], # guardo el snapshot del pedido
                "subtotal": subtotal,
                "discount_pct": pct,  # %
                "discount": discount_amount,  # monto
                "total": total,
                "external_order_id": payload.external_order_id,
                "client_id": payload.client_id,
                "tenant": tenant,  # trazabilidad de la farmacia
            }
            orders_coll.insert_one(order_doc)

            # Devuelve el objeto Order (ocultando _id)
            return OrderOut(**{k: v for k, v in order_doc.items() if k != "_id"})

        except HTTPException:
            # Errores conocidos (409/422) se propagan tal cual
            raise
        except Exception as e:
            # Cualquier error inesperado: intenta revertir los rebajos por seguridad
            for done_ndc, done_qty in reversed(decremented):
                catalog_coll.update_one({"package_ndc_11": done_ndc}, {"$inc": {"stock": done_qty}})
            raise HTTPException(status_code=500, detail={"message": f"Error al crear el pedido: {e}"})


# ----------- Asignación de endpoint GET ORDERS -----------
    @router.get("/orders", response_model=List[OrderOut], summary="Listar pedidos")
    def list_orders(
            since: Optional[datetime] = Query(None, description="Filtra confirmed_at >= since"),
            limit: int = Query(50, ge=1, le=200),
            offset: int = Query(0, ge=0),
            client_id: Optional[str] = Query(None, description="Filtra por client_id"),
    ):
        """
        Enlista órdenes confirmadas.
        - since: devuelve solo órdenes con confirmed_at >= since.
        - limit/offset: paginación simple (por defecto 50 resultados).
        Ordena por confirmed_at de forma descendente (más recientes primero).
        """
        filter_q = {}
        if since:
            filter_q["confirmed_at"] = {"$gte": since}
        if client_id:
            filter_q["client_id"] = client_id

        cursor = (
            orders_coll
            .find(filter_q, {"_id": 0})
            .sort([("confirmed_at", -1)])
            .skip(offset)
            .limit(limit)
        )
        # Pydantic valida/normaliza la salida al modelo Order
        return [OrderOut.model_validate(doc) for doc in cursor]

# ----------- Asignación de endpoint GET ORDER (ID) -----------
    @router.get("/orders/{order_id}", response_model=OrderOut, summary="Obtener pedido por ID")
    def get_order(order_id: str = Path(..., description="UUID del pedido")):
        """
        Devuelve una orden puntual por su identificador (order_id).
        404 si no existe.
        """
        doc = orders_coll.find_one({"order_id": order_id}, {"_id": 0})
        if not doc:
            raise HTTPException(status_code=404, detail={"message": "Pedido no encontrado"})
        return OrderOut(**doc)

# Devuelve el router listo para ser montado en la app principal
    return router


