#!/usr/bin/env python3
"""Register Kestra Flow as Pipeline in OpenMetadata"""

import os
import requests
import json
from urllib.parse import quote

OPENMETADATA_HOST = os.getenv("OPENMETADATA_HOST", "openmetadata_server:8585")
OPENMETADATA_TOKEN = os.getenv("OPENMETADATA_TOKEN", "").strip()
FLOW_NAME = os.getenv("FLOW_NAME", "solar_ingestion_test_ohne_skript")
NAMESPACE = os.getenv("NAMESPACE", "datamesh.tests")

def register_kestra_pipeline():
    """Create Kestra Flow as Pipeline in OpenMetadata"""
    
    if not OPENMETADATA_TOKEN:
        print("❌ Kein OPENMETADATA_TOKEN gesetzt.")
        return False
    
    api_url = f"http://{OPENMETADATA_HOST}/api/v1/pipelines"
    
    payload = {
        "name": f"{NAMESPACE}.{FLOW_NAME}",
        "displayName": f"Kestra: {FLOW_NAME}",
        "description": f"{FLOW_NAME} Ingestion Pipeline von Kestra",
        "service": "kestra",  # Muss zum Pipeline Service passen
        "sourceUrl": f"http://datamesh.mni.thm.de:8095/ui/main/flows/edit/{NAMESPACE}/{FLOW_NAME}/overview?filters[timeRange][EQUALS]=PT24H",
        "tasks": [
            {
                "name": "run_solar_script",
                "displayName": "Download & Send to Kafka",
                "taskType": "Python",
                "description": f"Downloads {FLOW_NAME} data and sends to Redpanda"
            },
            {
                "name": "index_metadata",
                "displayName": "Register in OpenMetadata",
                "taskType": "Python",
                "description": "Registers topic metadata in OpenMetadata"
            }
        ]
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENMETADATA_TOKEN}"
    }
    
    print(f"📝 Registriere Kestra Flow: {FLOW_NAME}...")
    
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        
        if response.status_code in [200, 201]:
            print(f"✅ Pipeline erfolgreich registriert")
            print(f"   Flow: {FLOW_NAME}")
            return True
        elif response.status_code == 409:
            print("ℹ️ Pipeline existiert bereits")
            return True
        else:
            print(f"⚠️ Response: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return False

if __name__ == "__main__":
    success = register_kestra_pipeline()
    exit(0 if success else 1)