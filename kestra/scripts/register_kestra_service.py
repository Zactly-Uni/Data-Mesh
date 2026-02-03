#!/usr/bin/env python3
"""Register Kestra as Pipeline Service in OpenMetadata"""

import os
import requests
import json

OPENMETADATA_HOST = os.getenv("OPENMETADATA_HOST", "openmetadata_server:8585")
OPENMETADATA_TOKEN = os.getenv("OPENMETADATA_TOKEN", "").strip()

def register_kestra_service():
    """Create Kestra Pipeline Service in OpenMetadata"""
    
    if not OPENMETADATA_TOKEN:
        print("❌ Kein OPENMETADATA_TOKEN gesetzt.")
        return False
    
    api_url = f"http://{OPENMETADATA_HOST}/api/v1/services/pipelineServices"
    
    payload = {
        "name": "kestra",
        "displayName": "Kestra Orchestration",
        "description": "Kestra workflow orchestration platform for Data Mesh pipelines",
        "serviceType": "Kestra",
        "connection": {
            "config": {
                "type": "CustomPipeline",
                "connectionOptions": {
                    "kestra_url": "http://datamesh.mni.thm.de:8095"
                }
            }
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENMETADATA_TOKEN}"
    }
    
    print("📝 Registriere Kestra Pipeline Service...")
    
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        
        if response.status_code in [200, 201]:
            print("✅ Kestra Service erfolgreich registriert")
            return True
        elif response.status_code == 409:
            print("ℹ️ Kestra Service existiert bereits")
            return True
        else:
            print(f"⚠️ Response: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return False

if __name__ == "__main__":
    success = register_kestra_service()
    exit(0 if success else 1)