import os
import requests
import geopandas as gpd
import json
from kafka import KafkaProducer

# --- KONFIGURATION ---
DOWNLOAD_URL = "https://solar-kataster-hessen.de/dl_shape/solareignung_geb_marburg.zip"
LOCAL_FOLDER = "downloads"
LOCAL_FILENAME = "solar_marburg.zip"
LOCAL_FILE_PATH = os.path.join(LOCAL_FOLDER, LOCAL_FILENAME)

# Redpanda Einstellungen (Interner Docker Name)
KAFKA_BROKER = 'redpanda-0:9092,redpanda-1:9092,redpanda-2:9092' 
TOPIC = 'solarkataster_hessen'

# ---------------------------------------------------------
# 1. DER DOWNLOADER (Dein Code + Fehlerbehandlung)
# ---------------------------------------------------------
def download_file_safe(url, file_path):
    # Ordner erstellen, falls nicht da
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Check: Haben wir die Datei schon?
    if os.path.exists(file_path):
        print(f"📦 Datei existiert bereits: {file_path}")
        # Optional: Hier könnte man prüfen, ob die Datei älter als X Tage ist
        return True

    print(f"📡 Starte Download von: {url}")
    
    # WICHTIG: Die Header, damit der Server uns nicht blockt
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://solar-kataster-hessen.de/",
        "Host": "solar-kataster-hessen.de"
    }

    try:
        response = requests.get(url, headers=headers, stream=True)
        
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Download erfolgreich: {file_path}")
            return True
        else:
            print(f"❌ Download fehlgeschlagen. Status Code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Kritischer Fehler beim Download: {e}")
        return False

# ---------------------------------------------------------
# 2. DIE HAUPTLOGIK (Geopandas -> Redpanda)
# ---------------------------------------------------------
def main():
    print("🚀 Solar-Importer gestartet...")

    # Schritt 1: Datei besorgen
    success = download_file_safe(DOWNLOAD_URL, LOCAL_FILE_PATH)
    
    if not success:
        print("Abburch: Konnte Shapefile nicht laden.")
        exit(1)

    # Schritt 2: Kafka Producer starten
    print("🔌 Verbinde zu Redpanda...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Konnte Redpanda nicht erreichen: {e}")
        exit(1)

    # Schritt 3: Shapefile direkt aus der ZIP lesen
    print("🌍 Lade Geodaten mit Geopandas (das kann kurz dauern)...")
    try:
        # Der Trick mit "zip://" spart das Entpacken
        gdf = gpd.read_file(f"zip://{LOCAL_FILE_PATH}")
        
        # WICHTIG: Umrechnen von UTM32 (Meter) in GPS (Lat/Lon) für Karten
        if gdf.crs.to_string() != "EPSG:4326":
            print("🔄 Rechne Koordinaten um in EPSG:4326...")
            gdf = gdf.to_crs(epsg=4326)
            
    except Exception as e:
        print(f"❌ Fehler beim Lesen der Geodaten: {e}")
        exit(1)

    print(f"📊 {len(gdf)} Gebäude gefunden. Sende Daten...")

    # Schritt 4: Senden
    count = 0
    for index, row in gdf.iterrows():
        try:
            # Wir bauen das JSON. 
            # .fillna(0) verhindert Fehler, wenn Felder leer sind
            properties = row.drop('geometry').fillna(0).to_dict()
            
            # Geometrie als GeoJSON extrahieren
            geometry = row.geometry.__geo_interface__

            message = {
                "id": f"marburg_{index}",
                "source": "Solarkataster Hessen",
                "geometry": geometry,
                "properties": properties 
            }

            producer.send(TOPIC, message)
            count += 1
            
            if count % 1000 == 0:
                print(f"   ... {count} Gebäude gesendet")

        except Exception as e:
            print(f"⚠️ Fehler bei Gebäude {index}: {e}")

    producer.flush()
    print(f"✅ FERTIG! {count} Gebäude liegen jetzt in Redpanda im Topic '{TOPIC}'.")

if __name__ == "__main__":
    main()
