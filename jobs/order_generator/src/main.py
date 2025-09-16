from __future__ import annotations
from .config import Config
from .catalog_client import CatalogClient
from .order_builder import OrderBuilder
from .runner import Runner

def main() -> None:
    """
    Punto de entrada principal del Job.
    Se encarga de los siguientes pasos:
    1) Carga configuración desde variables de entorno.
    2) Descarga los catálogos de cada farmacia (precarga inicial).
    3) Inicializa el OrderBuilder (para armar órdenes).
    4) Inicializa el Runner (para ejecutar el bucle con control de QPS).
    5) Ejecuta el loop de generación de órdenes.
    6) Muestra un resumen de métricas al final.
    """

    # 1) Configuración: trae las variables de entorno obligatorias y las que van por default
    cfg = Config.from_env()

    # 2) Precarga los catálogos locales de las tres farmacias
    #    Esto devuelve un diccionario con forma:
    #    {
    #      "farma_001": [ {"package_ndc_11": "...", "stock": X}, ... ],
    #      "farma_002": [...],
    #      "farma_003": [...]
    #    }
    client = CatalogClient(timeout=cfg.request_timeout)
    pools = client.preload_pools({
        "farma_001": cfg.catalog_url_farma_001,
        "farma_002": cfg.catalog_url_farma_002,
        "farma_003": cfg.catalog_url_farma_003,
    })

    # 3) Inicializa el generador de órdenes (OrderBuilder)
    builder = OrderBuilder(
        discount_pct=cfg.discount_pct,
        clients_max=cfg.clients_max,
        max_qty=cfg.max_qty,
        start_seq=cfg.seq_start,
    )

    # 4) Inicializa el ejecutor de órdenes (Runner)
    runner = Runner(
        base_url=cfg.farmahorra_base_url,
        timeout=cfg.request_timeout,
        qps_max=cfg.qps_max,
    )

    # 5) Ejecuta el bucle principal hasta ORDERS_TARGET
    runner.loop(orders_target=cfg.orders_target, pools=pools, builder=builder)

    # 6) Muesta métricas simples al finalizar
    runner.print_summary()

# Permite que el script se ejecute con "python -m src.main"
if __name__ == "__main__":
    main()
