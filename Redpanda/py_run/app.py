import streamlit as st
import json
import os
import importer 

# Lade Stammdaten aus der JSON Datei
@st.cache_data
def load_data():
    with open('hessen_structure.json', 'r', encoding='utf-8') as f:
        return json.load(f)

st.set_page_config(page_title="Hessen Solar Importer", layout="wide")

st.title("☀️ Solar-Kataster Hessen Importer")

# Daten laden
try:
    data = load_data()
except FileNotFoundError:
    st.error("hessen_structure.json nicht gefunden! Bitte Datei anlegen.")
    st.stop()

# --- 1. DATENSTRUKTUREN AUFBAUEN ---
bezirke_liste = [b['name'] for b in data]
kreise_map = {}      # Name -> Liste von Gemeinde-Objekten
gemeinde_map = {}    # Name -> Slug (mit Unterstrich)
bezirk_zu_kreis_map = {} # Bezirk -> Liste KreisNamen

for b in data:
    k_list = []
    for k in b['kreise']:
        k_list.append(k['name'])
        kreise_map[k['name']] = k['gemeinden']
        for g in k['gemeinden']:
            # Hier mappen wir den schönen Namen auf den Slug
            gemeinde_map[g['name']] = g['slug']
    bezirk_zu_kreis_map[b['name']] = k_list

# Listen für Selectboxen sortieren
alle_kreise = sorted(list(kreise_map.keys()))
alle_gemeinden = sorted(list(gemeinde_map.keys()))

# --- 2. UI AUSWAHL (3 SPALTEN) ---
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("1. Regierungsbezirk")
    selected_bezirke = st.multiselect("Wähle Bezirke (wählt alles darin aus)", bezirke_liste)

with col2:
    st.subheader("2. Landkreis / Kreisfreie Stadt")
    selected_kreise = st.multiselect("Wähle Kreise (wählt alle Gemeinden darin aus)", alle_kreise)

with col3:
    st.subheader("3. Gemeinde / Stadt")
    selected_gemeinden = st.multiselect("Wähle einzelne Gemeinden", alle_gemeinden)

# --- 3. DEDUPLIZIERUNGSLOGIK ---
# Wir nutzen ein Set, damit kein Slug doppelt vorkommt
final_slugs = set()
debug_info = []

# A. Bezirke auflösen
for bezirk in selected_bezirke:
    for k in data:
        if k['name'] == bezirk:
            for kreis in k['kreise']:
                for g in kreis['gemeinden']:
                    final_slugs.add(g['slug'])
    debug_info.append(f"Bezirk '{bezirk}': Alle Gemeinden hinzugefügt.")

# B. Kreise auflösen
for k_name in selected_kreise:
    gem_list = kreise_map.get(k_name, [])
    for g in gem_list:
        final_slugs.add(g['slug'])
    debug_info.append(f"Kreis '{k_name}': {len(gem_list)} Gemeinden hinzugefügt.")

# C. Einzelne Gemeinden
for g_name in selected_gemeinden:
    slug = gemeinde_map.get(g_name)
    if slug:
        final_slugs.add(slug)

# --- 4. ZUSAMMENFASSUNG & START ---
st.divider()

if not final_slugs:
    st.warning("Bitte wähle mindestens eine Region aus.")
else:
    st.info(f"Es werden **{len(final_slugs)}** einzigartige Datensätze (Gemeinden) geladen.")
    
    with st.expander("Details anzeigen (Technischer Slug)"):
        st.write(list(final_slugs))
        if debug_info:
            st.write(debug_info)

    if st.button("🚀 Import nach Redpanda starten", type="primary"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_area = st.container()

        # Callback Funktion
        def ui_callback(msg):
            with log_area:
                st.text(msg)
            status_text.text(msg)

        try:
            total = importer.run_batch_import(list(final_slugs), progress_callback=ui_callback)
            st.success(f"✅ Fertig! Insgesamt {total} Gebäude importiert.")
            progress_bar.progress(100)
        except Exception as e:
            st.error(f"Kritischer Fehler: {e}")