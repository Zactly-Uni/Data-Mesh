FROM python:3.10-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
# 1. System Libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev g++ librdkafka-dev pkg-config curl \
    && rm -rf /var/lib/apt/lists/*
# 2. Python Libs
RUN pip install --no-cache-dir --upgrade pip
COPY requirements_kestra.txt /app/requirements_kestra.txt
RUN pip install --no-cache-dir -r requirements_kestra.txt
# 3. Ordnerstruktur (WICHTIG: downloads Ordner erstellen!)
RUN mkdir -p /app/scripts /app/src /app/downloads
# 4. A) SRC: Hier kommen Import-Module rein (ingestion_uni)
COPY src/ingestion_uni.py /app/src/
# 4. B) SCRIPTS: Hier kommen alle ausführbaren Jobs rein
# Solar & Ladesäulen
COPY scripts/import_batch_solar.py /app/scripts/
COPY scripts/import_charging_stations.py /app/scripts/
# Metadaten-Skripte
COPY scripts/register_kestra_pipeline.py /app/scripts/
COPY scripts/register_kestra_service.py /app/scripts/
COPY scripts/index_metadata.py /app/scripts/
# 5. PYTHONPATH setzen
# Damit Python die 'ingestion_uni' in /app/src findet
ENV PYTHONPATH="/app/src"