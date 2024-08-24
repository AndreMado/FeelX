# Utilizar una imagen base de Python
FROM python:3.10-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar todo el proyecto, incluido requirements.txt
COPY ../ .

# Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Comando por defecto para ejecutar el productor
CMD ["python", "scripts/producer.py"]

