FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copio archivos necesarios
COPY requirements.txt ./
COPY farma_api.py routes_orders.py ./

# Instalo las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Puerto que expone FastAPI dentro del contenedor
EXPOSE 8000

# Comando para ejecutar el microservicio
CMD ["uvicorn", "farma_api:app", "--host", "0.0.0.0", "--port", "8000"]