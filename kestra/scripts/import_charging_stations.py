import sys
import os
import pandas as pd
import io
import re
from urllib.parse import urljoin

# WICHTIG: Importiert aus src/ingestion_uni.py
from ingestion_uni import get_kafka_producer, send_to_kafka, download_file, get_float_safe

LANDING_PAGE = "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html"

class ChargingStationImporter:
    def __init__(self):
        self.topic = os.getenv('KAFKA_TOPIC', 'charging_stations')
        self.topic_dlq = os.getenv('KAFKA_TOPIC_DLQ', f"{self.topic}_dlq")
        self.cities = [c.strip() for c in os.getenv('CITIES', '').split(',') if c.strip()]
        self.dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
        self.min_power = float(os.getenv('MIN_POWER', '0'))
        self.limit = int(os.getenv('LIMIT', '0'))
        
        self.producer = get_kafka_producer(self.dry_run)
        if not self.dry_run:
            print(f"🔌 Kafka bereit. Topic: {self.topic}")

    def get_csv_url(self):
        print(f"🕵️ Suche CSV auf {LANDING_PAGE}...")
        try:
            resp = download_file(LANDING_PAGE)
            match = re.search(r'href="([^"]*Ladesaeulenregister[^"]*\.csv[^"]*)"', resp.text, re.IGNORECASE)
            return urljoin(LANDING_PAGE, match.group(1)).replace('&amp;', '&') if match else None
        except Exception:
            return None

    def process(self):
        url = self.get_csv_url()
        if not url: print("❌ Keine URL gefunden"); sys.exit(1)

        print("⬇️ Lade CSV...")
        resp = download_file(url)
        content = resp.content.decode('latin-1', errors='replace')
        
        # Header Logik
        header_idx = 0
        for i, line in enumerate(content.split('\n')[:30]):
            if "Breitengrad" in line and "Bundesland" in line:
                header_idx = i; break
        
        csv_params = {'sep': ';', 'encoding': 'latin-1', 'decimal': ',', 'skiprows': header_idx, 'on_bad_lines': 'skip'}
        if self.limit > 0: csv_params['nrows'] = self.limit
        
        df = pd.read_csv(io.BytesIO(resp.content), **csv_params)
        df.columns = df.columns.str.strip()

        # Filtern
        if 'Bundesland' in df.columns:
            df = df[df['Bundesland'].astype(str) == 'Hessen'].copy()
        
        if self.cities:
            pattern = '|'.join(self.cities)
            df = df[df['Ort'].astype(str).str.contains(pattern, case=False, na=False)]

        if self.min_power > 0:
            df['p'] = pd.to_numeric(df['Nennleistung Ladeeinrichtung [kW]'], errors='coerce').fillna(0)
            df = df[df['p'] >= self.min_power]

        if 'Inbetriebnahmedatum' in df.columns:
            df['Inbetriebnahmedatum'] = pd.to_datetime(df['Inbetriebnahmedatum'], format='%d.%m.%Y', errors='coerce')
            df['Inbetriebnahmedatum_str'] = df['Inbetriebnahmedatum'].dt.strftime('%Y-%m-%d')
            # NaN (Not a Number) durch None ersetzen, damit JSON funktioniert
            df = df.where(pd.notnull(df), None)

        self.send_rows(df)

    def send_rows(self, df):
        success, dlq = 0, 0
        print(f"🚀 Sende {len(df)} Datensätze...")
        for _, row in df.iterrows():
            try:
                lat = get_float_safe(row.get('Breitengrad'))
                lon = get_float_safe(row.get('Längengrad'))
                if lat == 0 or lon == 0: raise ValueError("Koordinate 0")
                kw = get_float_safe(row.get('Nennleistung Ladeeinrichtung [kW]'))
            
                lk = "Normallader (<22kW)"
                if kw >= 149: lk = "HPC (>150kW)"
                elif kw >= 49: lk = "Schnelllader (>50kW)"
                elif kw >= 22: lk = "Lader (>=22kW)"

                msg = {
                    "ID": str(row.get('Ladeeinrichtungs-ID')),
                    "Betreiber": str(row.get('Betreiber')),
                    "Strasse": str(row.get('Straße')),
                    "PLZ": str(row.get('Postleitzahl')),
                    "Ort": str(row.get('Ort')),
                    "Bundesland": "Hessen",
                    "Breitengrad": lat, "Laengengrad": lon,
                    "Leistung_kW": kw, "Leistungsklasse": lk,
                    # --- HIER NEU EINFÜGEN ---
                    "Inbetriebnahme": row.get("Inbetriebnahmedatum_str"), 
                    # -------------------------
                    "Quelle": "BNetzA"
                }
                send_to_kafka(self.producer, self.topic, msg, self.dry_run)
                success += 1
            except Exception as e:
                dlq += 1
                send_to_kafka(self.producer, self.topic_dlq, {"err": str(e), "raw": row.to_dict()}, self.dry_run)

        if self.producer: self.producer.flush()
        print(f"🏁 Fertig. Success: {success} | DLQ: {dlq}")

if __name__ == "__main__":
    ChargingStationImporter().process()