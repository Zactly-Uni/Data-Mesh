import os
import requests
import geopandas as gpd
import json
from kafka import KafkaProducer
import time

# --- KONFIGURATION ---
# Die URL erwartet den Slug (z.B. "bad_endbach") an der Stelle {}
BASE_URL = "https://solar-kataster-hessen.de/dl_shape/solareignung_geb_{}.zip"
LOCAL_FOLDER = "downloads"
KAFKA_BROKER = 'redpanda-0:9092,redpanda-1:9092,redpanda-2:9092' 
TOPIC = 'solarkataster_hessen'

# Funktion zum Download
def download_file_safe(slug, progress_callback=None):
    # Hier wird der Unterstrich-Slug in die URL eingefügt
    url = BASE_URL.format(slug)
    file_path = os.path.join(LOCAL_FOLDER, f"solar_{slug}.zip")
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    if os.path.exists(file_path):
        if progress_callback: progress_callback(f"📦 Datei vorhanden: {slug}")
        return file_path

    if progress_callback: progress_callback(f"📡 Download: {slug} von {url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://solar-kataster-hessen.de/"
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return file_path
        else:
            if progress_callback: progress_callback(f"❌ Fehler {response.status_code} bei {slug} (URL: {url})")
            return None
    except Exception as e:
        if progress_callback: progress_callback(f"❌ Exception bei {slug}: {e}")
        return None

# Hauptfunktion für den Batch-Lauf
def run_batch_import(slug_list, progress_callback=print):
    # Kafka Producer einmalig initialisieren
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception:
        # Fallback für UI Tests ohne Kafka
        producer = None
        progress_callback("⚠️ Warnung: Kein Kafka verbunden (Dry Run)")

    total_buildings = 0

    for slug in slug_list:
        path = download_file_safe(slug, progress_callback)
        if not path:
            continue

        try:
            progress_callback(f"🌍 Lade Geodaten für {slug}...")
            # ZIP lesen ohne Entpacken
            gdf = gpd.read_file(f"zip://{path}")
            
            # Koordinatenumrechnung (Standardmäßig EPSG:25832 -> EPSG:4326)
            if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs(epsg=4326)
            
            # Senden
            count = 0
            if producer:
                for index, row in gdf.iterrows():
                    properties = row.drop('geometry').fillna(0).to_dict()
                    properties['region_slug'] = slug 
                    
                    message = {
                        "id": f"{slug}_{index}",
                        "source": "Solarkataster Hessen",
                        "geometry": row.geometry.__geo_interface__,
                        "properties": properties 
                    }
                    producer.send(TOPIC, message)
                    count += 1
                producer.flush()
            
            total_buildings += count
            progress_callback(f"✅ {slug}: {count} Gebäude verarbeitet.")
            
        except Exception as e:
            progress_callback(f"❌ Fehler beim Verarbeiten von {slug}: {e}")

    return total_buildings