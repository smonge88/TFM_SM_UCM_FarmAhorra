# TFM – Plataforma FarmaHorra 
**Estudiante: Samuel Monge Alvarado**

**Fecha de entrega: 18 de setiembre, 2025**

Este proyecto, tal y como se expuso en el trabajo escrito, se plantea mediante cinco módulos:
1. Captura de datos de medicamentos y creación de catálogos por Farmacia.
2. Lanzamiento de contenedores por Farmacia, a partir de los catálogos creados.
3. Creación de FarmAhorra como plataforma para hacer pedidos.
4. Simulación de pedidos en tiempo real.
5. Análisis y obtención de indicadores.

---
## Requisitos previos
- **Python 3.11+**
- **MongoDB** (local o en contenedor vía `docker-compose`)
- Instalar dependencias:
  ```bash
  pip install -r requirements.txt

---
## Módulo 1: Captura de datos de medicamentos y creación de catálogos por Farmacia.

Este módulo contiene una serie de **scripts de Python** para la descarga, limpieza, 
normalización y carga en MongoDB de datasets oficiales de medicamentos, 
con el objetivo de generar catálogos unificados por farmacia.  

La ejecución completa se orquesta mediante un pipeline que integra todas las etapas 
y valida su correcto funcionamiento con tests unitarios.

Los archivos contenidos dentro de la carpeta _scripts_ y _tests_ son los siguientes:

    ├── scripts/ # Scripts principales del pipeline
    │ ├── a_download_sets.py # Descarga datasets NADAC y NDC
    │ ├── b_clean_nadac.py # Limpieza del archivo NADAC
    │ ├── c_import_ndc_to_mongo.py # Importación del archivo NDC a MongoDB
    │ ├── d_clean_ndc.py # Limpieza de registros NDC
    │ ├── e_normalize_package_ndc.py # Normalización de package_ndc a 11 dígitos
    │ ├── f_add_selling_size.py # Extracción de selling_size desde descripción
    │ ├── g_join_nadac_with_products.py # Unión de NADAC y NDC enriquecidos
    │ ├── h_generate_catalogs.py # Generación de catálogos por farmacia en MongoDB
    │ └── z_run_pipeline.py # Script principal (pipeline completo)
    │
    ├── tests/ # Pruebas unitarias asociadas a los scripts
    │ ├── test_clean_nadac.py
    │ ├── test_download_sets.py
    │ ├── test_extract_selling_size.py
    │ └── test_normalize_package_ndc.py

**Ejecución automática con pipeline**

En lugar de correr cada script manualmente, ejecuto todo el pipeline con:

    python scripts/z_run_pipeline.py

Este script:
* Corre todos los tests unitarios en tests/. 
* Ejecuta los scripts en orden. 
* Genera al final el archivo products_enriched.csv y los catálogos en MongoDB.

**Tests**

Los tests se encuentran en la carpeta tests/ y validan las funciones críticas del pipeline.
Puedo ejecutarlos manualmente con:

    python -m unittest discover -s tests

También puedo dejar que z_run_pipeline.py los ejecute automáticamente antes de iniciar el pipeline.

**Resultados esperados**

Colecciones en MongoDB:

* products_cleaned 
* products_normalized 
* products_with_selling_size 
* products_enriched 
* catalog_<farmacia_id> (catálogos por farmacia)

---
## Módulo 2: Lanzamiento de contenedores por Farmacia, a partir de los catálogos creados.

Este módulo se encuentra inmerso en el directorio de _apis/farmacia_api_, el cual contiene 
la definición del microservicio Farmacia API, encargado de exponer el catálogo y 
las órdenes de cada farmacia simulada.

**farma_api.py**

Define la API principal con FastAPI.
* Expone el catálogo de productos de una farmacia (/catalog/products). 
* Permite obtener un producto específico por NDC de 11 dígitos (/catalog/products/{ndc}). 
* Carga dinámicamente la colección catalog_<farmacia_id> de MongoDB, según la variable de entorno ID_FARMACIA.
* Monta el router de órdenes definido en routes_orders.py.

**routes_orders.py**
Define el router de órdenes con endpoints para:
* POST /orders: crea una orden, valida stock y calcula totales.
* GET /orders: lista órdenes confirmadas con filtros y paginación.
* GET /orders/{order_id}: consulta una orden puntual. 
* Cada orden se guarda en una colección orders_<farmacia_id> de MongoDB.

**requirements.txt**

Lista todas las dependencias necesarias (fastapi, uvicorn, pymongo, python-dotenv), 
las cuales se instalan automáticamente al construir la imagen Docker.

**Dockerfile**

Define cómo se construye la imagen de la API:
* Expone el puerto 8000 (interno del contenedor).
* Ejecuta el servicio con uvicorn farma_api:app.

**Construcción de la imagen manualmente**

Desde la carpeta apis/farmacia_api:

    docker build -t user/farma_api:1.0.0 .

Subir a Docker Hub:

    docker push user/farma_api:1.0.0

Una vez que la imagen está en Docker Hub, puede ser usada para construir las Azure Container Apps por farmacia.

El archivo docker-compose.yaml permite levantar múltiples instancias de farmacias (ej. farma_001, farma_002, farma_003), cada una escuchando en un puerto distinto:

    docker-compose up --build

Para detener y limpiar imágenes, contenedores y volúmenes:

    docker-compose down --rmi all --volumes

**Links de prueba**

Una vez levantados los contenedores, se pueden probar los siguientes endpoints:

Catálogos locales:

http://localhost:8001/farma_001/catalog/products

http://localhost:8002/farma_002/catalog/products

http://localhost:8003/farma_003/catalog/products

Documentación interactiva (Swagger UI)

http://localhost:8001/docs

http://localhost:8002/docs

http://localhost:8003/docs

En Azure, las variables de entorno para crear los contenedores, 
donde van a correr cada farmacia, son las siguientes:
* ID_FARMACIA:


    farma_001, farma_002 o farma_003 -> según corresponda
* MONGO_URI: 

    ```
    mongodb+srv://USER:PASSWORD@mongodb-products.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false&maxIdleTimeMS=120000&authSource=admin
    ```

---
## Conexión de base de datos MongoDB con CosmosDB

Este MONGO_URI proviene de la base de datos de MongoDB ya conectada en CosmosDB.

Para conectar la base de datos en MongoDB con CosmosDB, debe hacerse lo siguiente:

1. Abrir una ventana de _PowerShell_
2. Por cada catálago de las farmacias, 
hacer un _dump_ de uno de los catálogos
(reemplazar farma_00X por el id de Farmacia correspondiente):

    ```
    $DumpDir = "C:\temp\dump_catalog_farma_00X_$(Get-Date -Format yyyyMMdd_HHmm)"
    New-Item -ItemType Directory -Force -Path $DumpDir | Out-Null
    mongodump `
      --uri "mongodb://localhost:27017" `
      --db ndc_db `
      --collection catalog_farma_003 `
      --out $DumpDir
    ```

3. Esto devuelve un path temporal donde se hizo el dump del catálogo. Anotarlo.

4. Luego, por cada _dump_, realizar un _restore_ por catálago. 
En $DumpDir usar el path temporal, devuelto del paso anterior.

    ```
    $DumpDir    = "C:\temp\dump_catalog_farma_003_20250908_1017"
    $SrvUri     = 'mongodb+srv://mongodb-products.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000'
    $CosmosUser = 'user'          # ingresar usuario con el que se creó la base de datos Azure CosmosDB
    $CosmosPass = 'password'    # ingresar contraseña usada para crear la base de datos Azure CosmosDB
    
    mongorestore `
      --uri $SrvUri `
      --username $CosmosUser `
      --password $CosmosPass `
      --nsFrom "ndc_db.catalog_farma_003" `
      --nsTo   "ndc_db.catalog_farma_003" `
      --drop `
      "$DumpDir\ndc_db\catalog_farma_003.bson"
    ```

5. Para verificar que la conexión de la base de datos se haya hecho correctamente,
establecer una conexión a través de MongoDB Compass con la base de datos de CosmosDB y revisar los distintos sets de datos.
6. Usar el Mongo_URI que se puede ver en los _connections strings_ de la configuración
de la base de datos en Azure.

Esta es la _Mongo_URI_ que se utiliza como variable de entorno 
en la configuración de cada ACA por farmacia.


---
## Módulo 3: Creación de FarmAhorra como plataforma para hacer pedidos.

Este módulo se desarrolla principalmente en la carpeta _apis/farmahorra_api_. Aquí, **app_farmahorra.py** implementa el orquestador central de FarmAhorra.

Su objetivo es recibir órdenes, enrutar la solicitud a la farmacia seleccionada (por ID), 
validar/confirmar el pedido en esa farmacia (donde se descuenta stock y se calculan precios) 
y guardar el resultado en una base de datos MongoDB/Cosmos (Mongo API).

Además, expone un endpoint para agregar catálogos de todas las farmacias y devolverlos en un único JSON.

**Endpoints clave**

    POST /orders -> recibe el pedido, llama a la farmacia id_farmacia, confirma y guarda.

    GET /orders/{external_order_id} -> obtiene un pedido consolidado por ID externo.

    GET /orders -> lista pedidos con filtros (id_farmacia, client_id), ordenados por confirmed_at.

    GET /products -> combina catálogos de farmacias.

**Variables de entorno**

    PHARMACIES (JSON) — variable obligatoria que mapea entre IDs de farmacia y su URL base.
    
    MONGO_URI (por defecto mongodb://localhost:27017/)

    MONGO_DB (por defecto fa_db)

    MONGO_COLLECTION (por defecto orders_farmahorra)

En Azure, las variables de entorno son las siguientes:
* PHARMACIES (JSON):

    ```
    {"farma_001":"https://container-farma-001.purpledesert-b43a4980.eastus.azurecontainerapps.io/", 
     "farma_002":"https://container-farma-002.purpledesert-b43a4980.eastus.azurecontainerapps.io/", 
     "farma_003":"https://container-farma-003.purpledesert-b43a4980.eastus.azurecontainerapps.io/"}
    ```

Estas URLs son tomadas de cada ACA creada por farmacia, en la sección de Overview.
* MONGO_URI: 

    ```
    mongodb+srv://USER:PASSWORD@mongodb-products.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false&maxIdleTimeMS=120000&authSource=admin
    ```
* MONGO_DB:

    ```
    fa_db
    ```
* MONGO_COLLECTION:

    ```
    orders_farmahorra
    ```

**requirements.txt**

Lista todas las dependencias necesarias (fastapi, uvicorn, httpx, motor), 
las cuales se instalan automáticamente al construir la imagen Docker.

**Dockerfile**

Define cómo se construye la imagen de la API:
* Expone el puerto 8080 (interno del contenedor).
* Ejecuta el servicio con uvicorn app_farmahorra:app.

**Construcción de la imagen manualmente**

Desde la carpeta apis/farmahorra_api:

    docker build -t user/app_farmahorra:1.0.0 .

Subir a Docker Hub:

    docker push user/app_farmahorra:1.0.0

---
## Módulo 4: Simulación de pedidos en tiempo real.

Este módulo levanta un Job batch (pensado para Azure Container Apps Jobs) 
para generar lotes de órdenes sintéticas a través de FarmAhorra.

Los archivos contenidos dentro de la carpeta _jobs/order_generator/src_ son los siguientes:

    ├── src/ # Scripts principales del pipeline
    │ ├── catalog_client.py
    │ ├── config.py
    │ ├── init.py
    │ ├── main.py
    │ ├── order_builder.py
    │ ├── runner.py

* config.py: 

Carga la configuración desde variables de entorno (función Config.from_env())
* catalog_client.py: 

Cliente HTTP para descargar y normalizar catálogos de las farmacias.
Devuelve un pool por farmacia con {package_ndc_11, stock}.

* order_builder.py: 

Construye el payload de una orden a partir del pool local, 
eligiendo aleatoriamente la farmacia, la cantidad y el producto.
Genera external_order_id y expone apply_local_decrement para 
descontar stock local si la orden fue aceptada.

* runner.py

Bucle principal del job. 
Controla el QPS, hace POST /orders a FarmAhorra, 
clasifica resultados (2xx, 4xx, 5xx, timeout),
aplica decremento local cuando corresponde y muestra métricas al final.

* main.py

Orquesta y organiza las funciones anteriores.

**requirements.txt**

Lista todas las dependencias necesarias (requests en este caso), 
las cuales se instalan automáticamente al construir la imagen Docker.

**Dockerfile**

Define cómo se construye la imagen de la Job.

**Construcción de la imagen manualmente**

Desde la carpeta jobs/:

    docker build -t user/order-generator:v1.2 .

Subir a Docker Hub:

    docker push user/order-generator:v1.2

En Azure, las variables de entorno son las siguientes:
* FARMAHORRA_BASE_URL:


    https://container-farmahorra.purpledesert-b43a4980.eastus.azurecontainerapps.io
    
* CATALOG_URL_FARMA_001:


     "https://container-farma-001.purpledesert-b43a4980.eastus.azurecontainerapps.io/"

* CATALOG_URL_FARMA_002:


     "https://container-farma-002.purpledesert-b43a4980.eastus.azurecontainerapps.io/"

* CATALOG_URL_FARMA_003:


     "https://container-farma-003.purpledesert-b43a4980.eastus.azurecontainerapps.io/"}

* ORDERS_TARGET: 50
* QPS: 2
* MAX_QTY: 2
* DISCOUNT_PCT: 5
* CLIENTS_MAX: 999
* REQUEST_TIMEOUT: 45 
* REFRESH_CATALOG_EVERY: 0
* SEQ_START: 0 (cada vez que ejecuto el job, lo actualizo con el consecutivo de # de orden)

---
## Módulo 5: Análisis y obtención de indicadores mediante Databricks y Power BI.

### Preparación del Data Lake en Azure (ADLS Gen2)

Se siguen estos pasos:
1. Creación de la Storage Account con Hierarchical Namespace habilitado,
llamada adlsfarmahorra y en la misma región del workspace de Databricks.
2. Creación del container principal, llamado datalake y que se encuentra
en esta URL:

    ```
    abfss://datalake@adlsfarmahorra.dfs.core.windows.net/
    ```

### Creación del Workspace de Azure Databricks (Premium)

Se siguen estos pasos:
1. Creación de Azure Databricks Service 
en el plan Premium (necesario para Unity Catalog).
2. Uso un Resource Group dedicado.

### Requerimientos para acceder al Data Lake desde Databricks

Se requiere lo siguiente:
1. Crear una User Assigned Managed Identity en Azure, llamada sc_farma_adls.
2. En la Storage Account (adlsfarmahorra), asignarle a esa identidad 
el rol de Storage Blob Data Contributor.
3. En Unity Catalog, crear el Storage Credential apuntando a esa identidad.
4. Crear la External Location apuntando al container del Data Lake, usando el URL ya mostrado.

### Creación del catálogo y capas

Desde la pestaña de Catalog, crear el catálogo llamado farma, 
el cual se relaciona con la external location del paso anterior.

Dentro de farma, construir manualmente los schemas de raw, bronze, silver y gold.

De esta forma, los notebooks pueden usar directamente rutas como farma.<schema>.<tabla> 
y escribir en el Data Lake sin montar DBFS manualmente.

### Notebooks del pipeline

Estos notebooks están directamente en el Workspace de Databricks,
pero aquí se encuentran guardados en la carpeta _notebooks_.

* 00_farmahorra_raw: ingesta desde Cosmos (Mongo API) -> guarda Delta en farma.raw.*.
* 01_farmahorra_bronze: creación de la tabla base de órdenes, removiendo duplicados y validando tipos de datos. -> farma.bronze.*.
* 02_farmahorra_silver: limpieza de las tablas eliminando columnas innecesarias y registros nulos. -> farma.silver.*.
* 03_farmahorra_gold: creación de tablas y vistas agregadas enfocadas en KPIs de negocio-> farma.gold.*.

Los cuatro Notebooks son activados en secuencia a través de un Job manual de Databricks.

### Adicionales

Algunos puntos adicionales relevantes:
* Para correr tanto los Notebooks como el Job, se puede crear un Cluster
asignándole ciertos recursos, o correrlo en _Serverless_.
* En el notebook 00_farmahorra_raw rellenar los widgets para conectar a Cosmos (API MongoDB),
usando estos datos de órdenes:

**COSMOS_URI**

    mongodb+srv://mongodb-products.global.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000

**COSMOS_USER y COSMOS_PASS**: usuario y contraseña con la que monté el MONGO API en CosmosDB,
y que se utilizó en los módulos anteriores.
