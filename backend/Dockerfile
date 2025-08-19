FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencias del sistema (opcionalmente agrega build-essential si lo necesitas)
RUN apt-get update && apt-get install -y --no-install-recommends \
  curl ca-certificates && \
  rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY app ./app

EXPOSE 8000

# Uvicorn con autoreload si montas el volumen del backend
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
