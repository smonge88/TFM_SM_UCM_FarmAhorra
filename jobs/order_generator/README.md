# order-generator (ACA Job)

Generador de órdenes para FarmAhorra con decremento local de stock y `quantity` en [1,2].

## Variables de entorno necesarias
- FARMAHORRA_BASE_URL
- CATALOG_URL_FARMA_001
- CATALOG_URL_FARMA_002
- CATALOG_URL_FARMA_003
- ORDERS_TARGET (default 100)
- QPS_MAX (default 2)
- MAX_QTY (default 2)
- DISCOUNT_PCT (default 5)
- CLIENTS_MAX (default 999)
- REQUEST_TIMEOUT (default 15)
- REFRESH_CATALOG_EVERY (default 0)
- SEQ_START=<último YYYYY>

# Construccion de la imagen con docker build -t mongesam/order-generator:v1.0 .
# Push de la imagen a DockerHub docker push mongesam/order-generator:v1.0

# Crear job en Azure a partir de esta imagen. 
# Ajustar variables de entorno y correr job desde la Interfaz de Azure.