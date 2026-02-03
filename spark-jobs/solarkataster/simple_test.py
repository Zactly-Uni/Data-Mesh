#!/usr/bin/env python3
"""
Einfacher ClickHouse Test OHNE Spark
"""
import requests
import json

def test_clickhouse():
    print("🔍 Teste ClickHouse Verbindung...")
    
    # Deine Daten
    host = "clickhouse-server"  # oder "localhost"
    port = "8123"
    user = "default"
    password = "zVNQxQbCK7nWUYYAiGsuxhf3RU0c04gK"
    
    url = f"http://{host}:{port}/"
    
    # Test 1: Ping
    print("\n1. Ping Test...")
    try:
        response = requests.get(f"{url}ping", auth=(user, password), timeout=5)
        print(f"   Status: {response.status_code}")
        print(f"   Antwort: {response.text}")
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return False
    
    # Test 2: Einfache Query
    print("\n2. SELECT 1 Test...")
    try:
        response = requests.post(
            url,
            data="SELECT 1 as test_value, 'Hello' as message",
            auth=(user, password),
            timeout=10
        )
        print(f"   Status: {response.status_code}")
        print(f"   Ergebnis: {response.text.strip()}")
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return False
    
    # Test 3: Datenbanken anzeigen
    print("\n3. Datenbanken auflisten...")
    try:
        response = requests.post(
            url,
            data="SHOW DATABASES",
            auth=(user, password),
            timeout=10
        )
        print(f"   Status: {response.status_code}")
        databases = response.text.strip().split('\n')
        print(f"   Gefundene DBs: {databases}")
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return False
    
    # Test 4: System-Tabellen (read-only)
    print("\n4. System-Tabellen check...")
    try:
        response = requests.post(
            url,
            data="SELECT name, engine FROM system.tables WHERE database='system' LIMIT 5",
            auth=(user, password),
            timeout=10
        )
        print(f"   Status: {response.status_code}")
        print(f"   Tabellen (Auszug):")
        for line in response.text.strip().split('\n'):
            print(f"     - {line}")
    except Exception as e:
        print(f"   ❌ Fehler: {e}")
        return False
    
    return True

if __name__ == "__main__":
    if test_clickhouse():
        print("\n" + "="*60)
        print("✅ Alle Tests erfolgreich!")
        print("   ClickHouse ist erreichbar und funktioniert.")
        print("\n   Nächster Schritt: PySpark installieren")
    else:
        print("\n" + "="*60)
        print("❌ Tests fehlgeschlagen")
        print("   Bitte überprüfe:")
        print("   1. Ist ClickHouse gestartet?")
        print("   2. Ist Port 8123 erreichbar?")
        print("   3. Sind Hostname/Passwort korrekt?")
