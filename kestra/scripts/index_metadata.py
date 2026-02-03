#!/usr/bin/env python3
"""
Universelles Skript zur Indexierung von Kafka Topics in OpenMetadata.
Funktioniert für JEDES Topic, ohne Hardcoding.
"""

import os
import json
import requests

# 1. Konfiguration laden
OPENMETADATA_HOST = os.getenv("OPENMETADATA_HOST", "openmetadata:8585")
OPENMETADATA_TOKEN = os.getenv("OPENMETADATA_TOKEN", "").strip()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "unknown")
SERVICE_NAME = os.getenv("SERVICE_NAME", "redpanda") # Standard: redpanda

def ensure_messaging_service():
    """Stellt sicher, dass der Redpanda/Kafka Service in OpenMetadata existiert"""
    # Verbindet sich mit der OpenMetadata API (Service-Endpunkt + Authentifizierung)
    api_url = f"http://{OPENMETADATA_HOST}/api/v1/services/messagingServices"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENMETADATA_TOKEN}"
    }
    
    service_payload = {
        "name": SERVICE_NAME,
        "displayName": "Redpanda Messaging",
        "description": "Redpanda Kafka-compatible messaging service",
        "serviceType": "Kafka",
        "connection": {
            "config": {
                "type": "Kafka",
                "bootstrapServers": "redpanda-0:9092",
                "schemaRegistryURL": ""
            }
        }
    }
    
    try:
        # Registrierung des Messaging-Service im Datenkatalog (Create, bei 409 existiert er bereits)
        response = requests.post(api_url, json=service_payload, headers=headers, timeout=30)
        if response.status_code in [200, 201]:
            print(f"✅ Messaging Service '{SERVICE_NAME}' erstellt")
        elif response.status_code == 409:
            print(f"ℹ️  Messaging Service '{SERVICE_NAME}' existiert bereits")
        else:
            print(f"⚠️  Service-Registrierung: {response.status_code}")
    except Exception as e:
        print(f"⚠️  Service konnte nicht registriert werden: {e}")

def index_generic_topic():
    print(f"📑 Starte generische Indexierung für: {KAFKA_TOPIC}")
    
    if not OPENMETADATA_TOKEN:
        print("❌ Kein Token. Abbruch.")
        return False
    
    # Stelle sicher, dass der Messaging Service existiert
    ensure_messaging_service()

    # 2. Automatische Namens-Generierung (Der "Schlau-Macher")
    # Aus "charging_station_hessen" wird "Charging Station Hessen"
    readable_name = KAFKA_TOPIC.replace('_', ' ').replace('-', ' ').title()

    # 3. Priorisierung: 
    # Hat Kestra uns einen speziellen Titel geschickt? Wenn ja, nimm den.
    # Wenn nein, nimm den automatisch generierten Namen.
    display_name = os.getenv("DATA_TITLE", readable_name)
    
    # Beschreibung in den Katalog schreiben (wird unten in das Topic-Payload übernommen)
    description = os.getenv(
        "DATA_DESCRIPTION", 
        f"Automatisch importierte Daten aus dem Kafka Topic '{KAFKA_TOPIC}'."
    )

    # 4. Datenpaket schnüren
    # Payload für das Topic im Datenkatalog inkl. Beschreibung
    payload = {
        "name": KAFKA_TOPIC,           # Technischer Name (z.B. charging_station)
        "displayName": display_name,   # Hübscher Name (z.B. Charging Station)
        "description": description,    # Textbeschreibung
        "service": SERVICE_NAME,       # Messaging Service (redpanda)
        "partitions": 1,
        "messageSchema": {
            "schemaType": "JSON",
            "schemaFields": []         # Schema lassen wir offen
        }
    }
    
    # Optional: Source URL hinzufügen, falls vorhanden
    source_url = os.getenv("SOURCE_URL")
    if source_url:
        payload["sourceUrl"] = source_url

    # 5. An OpenMetadata senden
    # Verbindet sich mit der OpenMetadata API (Topics-Endpunkt + Authentifizierung)
    api_url = f"http://{OPENMETADATA_HOST}/api/v1/topics"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENMETADATA_TOKEN}"
    }

    try:
        # Registrierung oder Aktualisierung des Topics im Datenkatalog (Upsert via PUT)
        response = requests.put(api_url, json=payload, headers=headers, timeout=30)
        
        if response.status_code in [200, 201]:
            print(f"✅ Erfolg! Topic '{KAFKA_TOPIC}' heißt jetzt '{display_name}'")
            return True
        else:
            print(f"⚠️ API Antwort: {response.status_code}")
            print(response.text)
            return True 
            
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return False

if __name__ == "__main__":
    index_generic_topic()