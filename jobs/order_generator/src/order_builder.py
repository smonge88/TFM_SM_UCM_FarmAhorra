from __future__ import annotations
import random
from typing import Dict, List, Optional

"""
Generador de órdenes sintéticas para ACA Job de FarmAhorra

Este módulo define las funciones para construir pedidos de prueba a partir de
catálogos precargados en memoria. No realiza llamadas HTTP; su única
responsabilidad es elegir productos con stock suficiente y armar el
payload que luego enviará el runner a `app_farmahorra` (POST /orders).

Componentes principales
-----------------------
- `CatalogPool`: diccionario que representa el catálogo disponible por farmacia para el Job.
- `FARM_TO_WWW`: mapeo desde `id_farmacia` al componente `WWW` usado en el
  `external_order_id` con formato `FAC-WWW-YYYYY`.
- `OrderBuilder`: clase que:
  * elige aleatoriamente una farmacia y una cantidad de items a comprar,
  * selecciona un producto del pool con `stock >= qty`,
  * arma el payload del pedido con el formato acordado,
  * lleva un contador local para generar el componente `YYYYY` del `external_order_id`,
  * expone `apply_local_decrement(...)` para descontar stock en el pool **solo** si la
    API respondió éxito (2xx), removiendo del pool los productos agotados.
"""


# Formato del diccionario CatalogPool
CatalogPool = Dict[str, List[dict]]   # { "farma_001": [ {package_ndc_11, stock}, ... ], ... }

# Mapeo entre id_farmacia y los dígitos WWW del external_order_id
FARM_TO_WWW = {
    "farma_001": "001",
    "farma_002": "002",
    "farma_003": "003",
}

class OrderBuilder:
    """
    Clase que construye la orden. Cuenta con funciones que realizan lo siguiente:
    - Elige aleatoriamente una farmacia y una cantidad
    - Toma del pool local precargado el catálogo de la farmacia
    - Selecciona un producto cuyo stock_local sea >= qty
    - Arma el payload de la orden con el formato acordado
    - Lleva un contador local para generar el YYYYY del external_order_id
    - Expone un método para aplicar el decremento local de stock tras éxito de orden
    """

    def __init__(self, discount_pct: int, clients_max: int, max_qty: int, start_seq: int = 0) -> None:
        # Parámetros de negocio que vienen de la config/ENV
        self.discount_pct = discount_pct # descuento fijo de 5
        self.clients_max = clients_max # CLI-001 .. CLI-999
        self.max_qty = max_qty # entre 1–2

        # Contador local para el componente YYYYY del external_order_id
        self.counter = int(start_seq)

    def _next_counter(self) -> str:
        """Devuelve el próximo YYYYY en formato de 5 dígitos con ceros a la izquierda."""
        self.counter += 1
        return f"{self.counter:05d}"

    @staticmethod
    def _rand_client_id(clients_max: int) -> str:
        """Genera un client_id aleatorio entre CLI-001 y CLI-999."""
        n = random.randint(1, clients_max)
        return f"CLI-{n:03d}"

    @staticmethod
    def _pick_farm() -> str:
        """Elige aleatoriamente una farmacia entre las tres disponibles."""
        return random.choice(["farma_001", "farma_002", "farma_003"])

    def _pick_qty(self) -> int:
        """ Elige aleatoriamente la cantidad del ítem a ordenar, que podrá ser entre 1 o 2."""
        return random.randint(1, max(1, self.max_qty))

    @staticmethod
    def _select_candidate(pool: List[dict], qty: int) -> Optional[int]:
        """
        Dado el 'pool' de la lista de productos de una farmacia y una qty,
        busca índices de productos con stock_local >= qty y elige uno al azar
        Devuelve el índice elegido o None si no hay candidatos.
        """
        candidates = [i for i, p in enumerate(pool) if p["stock"] >= qty]
        if not candidates:
            return None
        return random.choice(candidates)

    def build_order(self, pools: CatalogPool) -> tuple[Optional[dict], Optional[tuple[str, int, int]]]:
        """
        Intenta construir la orden:
        - Elige una farmacia y cantidad.
        - Selecciona un producto del catálogo local de la farmacia con stock suficiente.
        - Arma el payload según el formato pedido.
        - Devuelve  si logró elegir un producto,
          o None si no encontró candidato (stock local insuficiente).

        info_decremento = (farm_id, idx, qty) para que el Runner pueda
        aplicar el decremento local solo si la API devuelve éxito (2xx).
        """
        # 1) Elige aleatoriamente la farmacia (esto define también WWW)
        farm_id = self._pick_farm()

        # 2) Elige una cantidad aleatoria (1..max_qty)
        qty = self._pick_qty()

        # 3) Toma el catálogo local (pool) de la farmacia respectiva
        pool = pools.get(farm_id, [])

        # 4) Busca un candidato con stock_local >= qty
        idx = self._select_candidate(pool, qty)
        if idx is None:
            # No hay producto con stock suficiente en esa farmacia para esa qty
            # El Runner lanza "no_local_candidate" y sigue con otra orden
            return None, None

        # 5) Arma identificadores
        ndc = pool[idx]["package_ndc_11"]
        yyyyy = self._next_counter()  # contador local global a la corrida
        www = FARM_TO_WWW[farm_id] # 001 / 002 / 003
        external_order_id = f"FAC-{www}-{yyyyy}"  # FAC-WWW-YYYYY
        client_id = self._rand_client_id(self.clients_max)

        # 6) Construye el payload de la orden con el formato acordado
        payload = {
            "id_farmacia": farm_id,
            "external_order_id": external_order_id,
            "client_id": client_id,
            "discount_pct": self.discount_pct,
            "items": [
                {"package_ndc_11": ndc, "quantity": qty}
            ],
        }

        # 7) Devuelve también la info para aplicar el decremento local tras éxito
        return payload, (farm_id, idx, qty)

    @staticmethod
    def apply_local_decrement(pools: CatalogPool, farm_id: str, idx: int, qty: int) -> None:
        """
        Aplica el decremento local del stock en memoria solo si la API devolvió 2xx.
        - Resta la qty al producto seleccionado.
        - Si el stock llega a 0 o menos, se remueve del pool para evitar que vuelva a elegirse.
        """
        pools[farm_id][idx]["stock"] -= qty
        if pools[farm_id][idx]["stock"] <= 0:
            # Quita el producto agotado para acelerar futuras selecciones
            pools[farm_id].pop(idx)
