from __future__ import annotations
import requests
from typing import Dict, List

"""
Cliente de catálogos para el Job de órdenes (ACA)

Este módulo provee un cliente HTTP para consultar y normalizar los
catálogos de las farmacias que expone cada microservicio (`/catalog/products`).

- API principal:
  - `CatalogClient(timeout=15.0)`
  - `fetch_catalog(url) -> list[Product]`
      Hace GET a la URL dada, valida que la respuesta sea una lista JSON y
      filtra solo ítems con `package_ndc_11` (str) y `stock` (int > 0).
  - `preload_pools(urls: dict[str, str]) -> CatalogPool`
      Descarga y arma el pool para múltiples farmacias `{id_farmacia: url}`.

- Errores:
  - Puede lanzar `requests.HTTPError` ante códigos no 2xx.
  - `ValueError` si el catálogo no es una lista JSON.
"""


Product = dict # Representa un producto del catálogo
CatalogPool = Dict[str, List[Product]]  # Diccionario: id_farmacia -> lista de productos

class CatalogClient:
    """
    Cliente HTTP  para obtener y normalizar los catálogos de las farmacias.
    Se encarga de:
    - Llamar a los endpoints de cada farmacia (GET).
    - Validar que la respuesta sea una lista JSON.
    - Filtrar productos válidos que tengan package_ndc_11 y stock > 0.
    """

    def __init__(self, timeout: float = 15.0) -> None:
        # Timeout en segundos para las requests HTTP
        self.timeout = timeout

    def fetch_catalog(self, url: str) -> list[Product]:
        """
        Descarga y normaliza el catálogo de una farmacia.
        - Hace GET a la URL recibida.
        - Verifica que sea un JSON tipo lista.
        - Filtra solo productos que tengan:
          - 'package_ndc_11' (string)
          - 'stock' (int > 0)
        - Devuelve lista de productos con solamente package_ndc_11 y stock
        """
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()  # lanza excepción si la respuesta no es 2xx
        data = resp.json()

        if not isinstance(data, list):
            raise ValueError("El catálogo debe ser una lista JSON")

        norm: list[Product] = []
        for item in data:
            ndc = item.get("package_ndc_11")
            stock = item.get("stock")
            # valida que el NDC sea string y que stock sea un entero positivo
            if isinstance(ndc, str) and isinstance(stock, int) and stock > 0:
                # guarda package_ndc_11 y stock para el job
                norm.append({"package_ndc_11": ndc, "stock": stock})
        return norm

    def preload_pools(self, urls: dict[str, str]) -> CatalogPool:
        """
        Descarga los catálogos de todas las farmacias y construye el 'pool'.
        - Recibe un diccionario {id_farmacia: url_catalogo}
        - Para cada farmacia:
          - Llama a fetch_catalog(url)
          - Guarda la lista resultante en pools[farm_id]
        - Devuelve pools con forma:
          {
            "farma_001": [{ndc, stock}, ...],
            "farma_002": [...],
            "farma_003": [...]
          }
        """
        pools: CatalogPool = {}
        for farm_id, url in urls.items():
            pool = self.fetch_catalog(url)
            pools[farm_id] = pool
        return pools
