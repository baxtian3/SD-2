# Usamos una imagen base de Python 3.8
FROM python:3.9-slim

# Establecemos un directorio de trabajo
WORKDIR /app

# Copiamos los requerimientos para instalar las bibliotecas necesarias
COPY . .

# Instalamos las bibliotecas necesarias
RUN pip install confluent-kafka




# Establecemos el comando para ejecutar el script cuando se inicie el contenedor
CMD ["python", "inscripcion.py"]
