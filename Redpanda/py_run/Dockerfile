FROM python:3.11-slim

WORKDIR /app

# 1. System-Updates & Abhängigkeiten für Geo-Bibliotheken
# geopandas/fiona benötigen oft C++ Compiler oder GDAL-Libs im System
RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Python Dependencies installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Deine App-Dateien kopieren (WICHTIG: JSON und beide Skripte)
COPY importer.py .
COPY app.py .
COPY hessen_structure.json .

# 4. Streamlit Port freigeben
EXPOSE 8509

# 5. Healthcheck (optional, aber gut für Docker)
#HEALTHCHECK CMD curl --fail http://localhost:8509/_stcore/health || exit 1

# 6. Der Startbefehl für Streamlit
# Wir binden an 0.0.0.0, damit du von außen drauf zugreifen kannst
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
