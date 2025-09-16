from __future__ import annotations
import time
import requests
from typing import Dict

"""
Runner del ACA Job para generación de órdenes en lote

Este módulo define el loop Runner que envía órdenes sintéticas al orquestador
de FarmAhorra respetando un límite de solicitudes por segundo (QPS). El Runner
utiliza un un `OrderBuilder` (para armar el payload y elegir productos con
stock suficiente desde un `CatalogPool`) y publica cada pedido vía
`POST {base_url}/orders`.

Consideraciones
-----------------
- Respeta el umbral de QPS (`qps_max`) aplicando pausas entre requests.
- Construye órdenes con `builder.build_order(pools)` y maneja el caso
  `no_local_candidate` cuando no hay stock suficiente en el pool local.
- Envia la orden al orquestador (`_post_order`) y clasifica la respuesta.
- Emite un resumen final de métricas.

Interfaz principal
------------------
- `Runner`:
    Configura la URL del orquestador, el timeout por request y el límite QPS.
- `loop`
    Ejecuta el ciclo hasta alcanzar `orders_target`.
- `print_summary()`
    Imprime un resumen de métricas acumuladas.
"""

class Runner:
    """
    Clase que ejecuta el bucle principal de generación de órdenes.
    Tiene las siguientes funciones:
    - Recibir un OrderBuilder y un pool de catálogos.
    - Generar órdenes hasta llegar a ORDERS_TARGET.
    - Respetar el límite de QPS (queries per second).
    - Hacer POST /orders contra FarmAhorra.
    - Manejar errores (4xx, 5xx, timeouts).
    - Aplicar el decremento local solo en órdenes exitosas.
    - Al final mostrar un resumen simple de métricas.
    """

    def __init__(self, *, base_url: str, timeout: float, qps_max: float) -> None:
        # URL base del API de FarmAhorra (ej: https://container-farmahorra... )
        self.base_url = base_url.rstrip("/")  # quito el / al final por consistencia
        self.timeout = timeout

        # Control del QPS
        self.qps_max = max(0.1, qps_max)   # evita división entre cero
        self.min_interval = 1.0 / self.qps_max  # tiempo mínimo entre requests

        # Diccionario de métricas acumuladas
        self.stats: Dict[str, int] = {
            "attempted": 0, # Órdenes que se intentaron construir
            "created": 0, # Órdenes aceptadas por la API (2xx)
            "failed_4xx": 0, # Fallos de validación o stock (errores cliente)
            "failed_5xx": 0,  # Errores de servidor o de red
            "failed_timeout": 0, # Timeouts de requests
            "no_local_candidate": 0, # No se pudo elegir producto local (sin stock suficiente en pool)
        }

    def _post_order(self, payload: dict) -> int:
        """
        Hace POST de una orden al endpoint /orders.
        Devuelve el status_code de la respuesta (200, 409, 500, etc).
        """
        url = f"{self.base_url}/orders"
        resp = requests.post(url, json=payload, timeout=self.timeout)
        return resp.status_code

    def loop(self, *, orders_target: int, pools: dict, builder) -> None:
        """
        Bucle principal:
        - Itera hasta ORDERS_TARGET.
        - Genera un payload con OrderBuilder.
        - Si no hay candidato local (producto con stock suficiente), incrementa métrica y sigue.
        - Respeta el QPS, aplica un sleep si hace falta.
        - Hace POST y clasifica el resultado.
        - Si fue éxitoso, aplica decremento local.
        """
        last_ts = 0.0  # timestamp del último request

        for _ in range(orders_target):
            self.stats["attempted"] += 1

            # 1) Se intenta construir la orden
            payload, dec_info = builder.build_order(pools)
            if payload is None:
                # Si no hay stock suficiente en la farmacia elegida
                self.stats["no_local_candidate"] += 1

                # Se aplica un sleep por precaución
                now = time.perf_counter()
                sleep_for = max(0.0, self.min_interval - (now - last_ts))
                if sleep_for > 0:
                    time.sleep(sleep_for)
                last_ts = time.perf_counter()
                continue

            # 2) Control de ritmo antes del POST
            now = time.perf_counter()
            sleep_for = max(0.0, self.min_interval - (now - last_ts))
            if sleep_for > 0:
                time.sleep(sleep_for)

            # 3) Se hace POST de las orders
            status = 0
            try:
                status = self._post_order(payload)
            except requests.Timeout:
                self.stats["failed_timeout"] += 1
            except requests.RequestException:
                # Otros errores de requests
                self.stats["failed_5xx"] += 1
            finally:
                last_ts = time.perf_counter()  # actualiza timestamp tras intento

            # 4) Se clasifica el resultado
            if 200 <= status < 300:
                # Éxito: se creó la orden
                self.stats["created"] += 1

                # Se aplica decremento local de stock
                farm_id, idx, qty = dec_info  # type: ignore
                builder.apply_local_decrement(pools, farm_id, idx, qty)

            elif 400 <= status < 500:
                # Fallo lógico por validación o stock insuficiente real.
                self.stats["failed_4xx"] += 1

            elif status >= 500 or status == 0:
                # Error técnico por servidor o red
                self.stats["failed_5xx"] += 1

    def print_summary(self) -> None:
        """
        Imprime métricas simples al final de la corrida.
        Útil para ver si el Job generó suficientes órdenes y cuántas fallaron.
        """
        s = self.stats
        print("\n=== ORDER-GENERATOR SUMMARY ===")
        print(f"attempted:        {s['attempted']}")
        print(f"created (2xx):    {s['created']}")
        print(f"failed_4xx:       {s['failed_4xx']}")
        print(f"failed_5xx:       {s['failed_5xx']}")
        print(f"failed_timeout:   {s['failed_timeout']}")
        print(f"no_local_candidate:{s['no_local_candidate']}")
        print("================================\n")
