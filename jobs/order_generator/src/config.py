import os
from dataclasses import dataclass

"""
Configuración del ACA Job para generación de órdenes en batch

Este módulo define la dataclass `Config`, responsable de cargar toda la configuración
del Job que genera órdenes en lote hacia las APIs de FarmAhorra y farmacias.

Los valores se leen desde las variables de entorno, por lo que funciona igual
en ejecución local que en Azure Container Apps Jobs.
"""

@dataclass
class Config:
    """
    Clase con la configuración del Job.
    La idea es cargar parámetros desde variables de entorno (ENV) ya sea en
    local o en Azure ACA Jobs.
    """
    # URLs de los endpoints expuestos en las Container Apps
    farmahorra_base_url: str
    catalog_url_farma_001: str
    catalog_url_farma_002: str
    catalog_url_farma_003: str

    # Parámetros de control
    orders_target: int = 100  # Cantidad total de órdenes a generar
    qps_max: float = 3.0  # Límite de solicitudes por segundo (QPS = queries per second) para no saturar el contenedor
    max_qty: int = 2  # Cantidad máxima de productos a solicitar por ítem
    discount_pct: int = 5  # Descuento fijo de Farmahorra
    clients_max: int = 999  # Máximo número de clientes (CLI-001 hasta CLI-999)
    request_timeout: float = 15.0  # Timeout en segundos por request HTTP
    refresh_catalog_every: int = 0  # Refresca catálogos cada 0 órdenes

    # Semilla del consecutivo YYYYY (FAC-WWW-YYYYY)
    seq_start: int = 0  # Manual: último YYYYY usado (default 0)

    @staticmethod
    def _get_env(name: str, required: bool = True, default: str | None = None) -> str:
        """
        Función auxiliar para leer variables de entorno.
        """
        val = os.getenv(name, default)
        return val

    @classmethod
    def from_env(cls) -> "Config":
        """
        Carga una instancia de Config leyendo todas las variables de entorno.
        Esto es lo que usa `main.py` al inicio.
        """
        return cls(
        farmahorra_base_url=cls._get_env("FARMAHORRA_BASE_URL"),
        catalog_url_farma_001=cls._get_env("CATALOG_URL_FARMA_001"),
        catalog_url_farma_002=cls._get_env("CATALOG_URL_FARMA_002"),
        catalog_url_farma_003=cls._get_env("CATALOG_URL_FARMA_003"),
        orders_target=int(os.getenv("ORDERS_TARGET", "100")),
        qps_max=float(os.getenv("QPS_MAX", "3")),
        max_qty=int(os.getenv("MAX_QTY", "2")),
        discount_pct=int(os.getenv("DISCOUNT_PCT", "5")),
        clients_max=int(os.getenv("CLIENTS_MAX", "999")),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", "15")),
        refresh_catalog_every=int(os.getenv("REFRESH_CATALOG_EVERY", "0")),
        seq_start=int(os.getenv("SEQ_START", "0")),
        )