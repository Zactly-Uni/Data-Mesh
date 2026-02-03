import pandas as pd
import json

# Dateiname
input_file = 'stationlist_synoptic_germany (1).csv'

# CSV lesen
df = pd.read_csv(input_file, sep=';', dtype=str)

stations_lookup = {}

for _, row in df.iterrows():
    # Wir nehmen die Kennung EXAKT so, wie sie in der Datei steht.
    # Kein .zfill(), kein Auffüllen. Nur Leerzeichen am Rand weg.
    raw_id = str(row['Kennung']).strip()
    
    station_data = {
        "name": row['Stationsname'].strip(),
        "lat": row['Geog_Breite'].strip(),
        "lon": row['Geog_Laenge'].strip()
    }
    
    stations_lookup[raw_id] = station_data

# Speichern
with open('stations.json', 'w', encoding='utf-8') as f:
    json.dump(stations_lookup, f, indent=2, ensure_ascii=False)

print(f"Erledigt! {len(stations_lookup)} Stationen 1:1 übernommen.")