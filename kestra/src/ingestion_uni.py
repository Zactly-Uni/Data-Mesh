import os
import sys
import json #? brauche ich das noch ?
import requests
from kafka import KafkaProducer

# Gemeinsame Konstanten
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def get_kafka_producer(dry_run=False):
    """Kafka Producer Factory mit Error Handling."""
    if dry_run:
        return None
    
    broker = os.getenv('KAFKA_BROKER', 'redpanda-0:9092')
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        return producer
    except Exception as e:
        print(f"❌ Critical: Kafka Verbindung fehlgeschlagen ({broker}): {e}")
        sys.exit(1)

def send_to_kafka(producer, topic, data, dry_run=False):
    """Sendet Daten oder simuliert es."""
    if dry_run or producer is None:
        return
    try:
        producer.send(topic, data)
    except Exception as e:
        print(f"⚠️ Fehler beim Senden an {topic}: {e}")
        raise e

def download_file(url, local_path=None):
    """Download Helper: Gibt Response zurück oder speichert Datei."""
    try:
        r = requests.get(url, headers=HEADERS, stream=True)
        r.raise_for_status()
        
        if local_path:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            return local_path
        else:
            return r
    except Exception as e:
        print(f"❌ Download Fehler bei {url}: {e}")
        raise e

def get_float_safe(val):
    """Clean Code Helper: Safe Float Conversion."""
    import pandas as pd
    if pd.isna(val) or val == '': return 0.0
    if isinstance(val, (int, float)): return float(val)
    try:
        return float(str(val).replace(',', '.'))
    except ValueError:
        return 0.0