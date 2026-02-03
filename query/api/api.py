import os
import requests
import trino
import io
import logging
import time
import pandas as pd
from fastapi import HTTPException, FastAPI
from fastapi.responses import StreamingResponse
from google.protobuf.struct_pb2 import Struct

METABASE_URL = os.getenv("METABASE_URL")
METABASE_KEY = os.getenv("METABASE_KEY")

TRINO_HOST = os.getenv("TRINO_HOST")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "api_user")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "clickhouse")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")

def get_metabase_session_token():
    url = f"{METABASE_URL}/api/session"
    payload = {
        "username": os.getenv("METABASE_USERNAME"),
        "password": os.getenv("METABASE_PASSWORD")
    }

    response  = requests.post(url, json=payload)

    if response.status_code != 200:
        raise Exception(f"Metabase Login fehlgeschlagen: {response.text}")
    
    return response.json()["id"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def success_response(card_id, source, format, rows, data):
    return {
        "status": "ok",
        "card_id": card_id,
        "source": source,
        "format": format,
        "rows": rows,
        "data": data
    }

app = FastAPI()

@app.get("/debug/card/{card_id}")
def debug_card(card_id: int):
    url = f"{METABASE_URL}/api/card/{card_id}"
    headers = {"X-Metabase-Session": get_metabase_session_token()}
    response = requests.get(url, headers=headers)
    return response.json()

@app.get("/query/{card_id}")
def query_card(
    card_id: int,
    source: str = "trino",
    format: str = "json"
):
    source = source.lower()
    format = format.lower()

    if source not in ["trino", "metabase"]:
        raise HTTPException(400, "Source muss 'trino' oder 'metabase' sein")

    if format not in ["json", "csv", "xlsx", "parquet", "sql", "mbql", "protobuf"]:
        raise HTTPException(400, "Ungültiges Format")

    # 1. Metabase-Exporte müssen zuerst behandelt werden
    if source == "metabase":
        if format == "csv":
            data = get_metabase_csv(card_id)
            return StreamingResponse(
                io.BytesIO(data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=card_{card_id}.csv"}
            )

        if format == "xlsx":
            data = get_metabase_xlsx(card_id)
            return StreamingResponse(
                io.BytesIO(data),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename=card_{card_id}.xlsx"}
            )

        # Metabase unterstützt nur CSV/XLSX
        raise HTTPException(400, "Metabase unterstützt nur csv/xlsx für Exporte")

    # 2. Card laden (nur für Trino oder SQL/MBQL)
    card = load_metabase_card(card_id)

    # 3. SQL oder MBQL direkt zurückgeben
    if format == "sql":
        sql = get_metabase_sql_from_card(card)
        return {"sql": sql}

    if format == "mbql":
        mbql = get_metabase_mbql_from_card(card)
        return {"mbql": mbql}

    # 4. Trino-Query ausführen
    sql = get_metabase_sql_from_card(card)
    df = run_trino_query(sql, card_id)

    # 5. Trino-Exporte
    if format == "json":
        data = df_to_json(df)
        return success_response(
            card_id=card_id,
            source=source,
            format=format,
            rows=len(data),
            data=data
        )

    if format == "csv":
        data = df_to_csv(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=card_{card_id}.csv"}
        )

    if format == "xlsx":
        data = df_to_xlsx(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=card_{card_id}.xlsx"}
        )

    if format == "parquet":
        data = df_to_parquet(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename=card_{card_id}.parquet"}
        )
    
    if format == "protobuf":
        data = df_to_protobuf(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename=card_{card_id}.pb"}
    )


    raise HTTPException(500, "Interner Fehler: Format wurde nicht verarbeitet")


def load_metabase_card(card_id: int) -> dict:
    logger.info(f"Loading Metabase card {card_id}")
    # 1. Prüfen ob ENV Variablen gesetzt sind
    if not METABASE_URL or not METABASE_KEY:
        raise HTTPException(status_code=500, detail="Metabase ENV Variablen fehlen!")
    
    # 2. URL und Header bauen
    url = f"{METABASE_URL}/api/card/{card_id}"
    headers = {"X-Metabase-Session": get_metabase_session_token()}

    # 3. Anfrage an Metabase senden
    response = requests.get(url, headers=headers)

    # 4. Fehlerbehandlung
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Metabase API Fehler: {response.text}")
    
    # 5. JSON der kompletten card zurückgeben
    return response.json()

def get_metabase_sql_from_card(card: dict) -> str:
    dq = card.get("dataset_query", {})

    # Neues Format (Metabase 0.47+)
    if "stages" in dq:
        stages = dq.get("stages", [])
        if stages:
            native = stages[0].get("native")
            if isinstance(native, dict) and "query" in native:
                return native["query"]
            if isinstance(native, str):
                return native

    # Klassisches Format
    if "native" in dq:
        native = dq["native"]
        if isinstance(native, dict) and "query" in native:
            return native["query"]
        if isinstance(native, str):
            return native

    raise HTTPException(status_code=500, detail="Card enthält keine native SQL Query")

def get_metabase_mbql_from_card(card: dict) -> dict:
    dq = card.get("dataset_query", {})

    # 1. Neues Format (Metabase 0.47+): MBQL steckt in stages[0]
    if "stages" in dq:
        stages = dq.get("stages", [])
        if stages:
            stage = stages[0]

            # Wenn es eine MBQL-Stage ist (wie bei deiner Card 44)
            if stage.get("lib/type") == "mbql.stage/mbql":
                return stage

            # Falls Metabase später weitere Varianten einführt
            if "query" in stage and stage["query"] is not None:
                return stage["query"]

    # 2. Klassisches MBQL-Format (vor 0.47)
    if "query" in dq and dq["query"] is not None:
        return dq["query"]

    raise HTTPException(
        status_code=500,
        detail="Card enthält keine MBQL Query"
    )

def get_metabase_csv(card_id: int) -> bytes:
    card = load_metabase_card(card_id)
    dq = card["dataset_query"]

    if "stages" not in dq:
        raise HTTPException(500, "Card enthält keine MBQL-Stages")

    stage = dq["stages"][0]

    # Minimal gültiges MBQL Query
    mbql_query = {
    "database": dq["database"],
    "source-table": stage.get("source-table"),
    "aggregation": stage.get("aggregation"),
    "breakout": stage.get("breakout"),
    "filter": stage.get("filter"),
    "order-by": stage.get("order-by"),
    "limit": stage.get("limit"),
}


    payload = {
        "database": dq["database"],
        "type": "query",
        "query": mbql_query,
        "parameters": []
    }

    # WICHTIG: Debug-Ausgabe
    print("=== CSV PAYLOAD ===")
    print(payload)
    print("====================")

    url = f"{METABASE_URL}/api/dataset/csv"
    headers = {"X-Metabase-Session": get_metabase_session_token()}

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        raise HTTPException(500, f"Metabase CSV Fehler: {response.text}")

    return response.content

def get_metabase_xlsx(card_id: int) -> bytes:
    card = load_metabase_card(card_id)
    dq = card["dataset_query"]

    if "stages" not in dq:
        raise HTTPException(500, "Card enthält keine MBQL-Stages")

    stage = dq["stages"][0]

    mbql_query = {
        "database": dq["database"],
        "source-table": stage.get("source-table"),
        "aggregation": stage.get("aggregation"),
        "breakout": stage.get("breakout"),
        "filter": stage.get("filter"),
        "order-by": stage.get("order-by"),
        "limit": stage.get("limit"),
    }

    payload = {
        "database": dq["database"],
        "type": "query",
        "query": mbql_query,
        "parameters": []
    }

    print("=== XLSX PAYLOAD ===")
    print(payload)
    print("====================")

    url = f"{METABASE_URL}/api/dataset"
    headers = {
        "X-Metabase-Session": get_metabase_session_token(),
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        raise HTTPException(500, f"Metabase XLSX Fehler: {response.text}")

    return response.content

def run_trino_query(sql: str, card_id: int) -> pd.DataFrame:
    logger.info(f"Executing SQL for card {card_id}: {sql}")

    start = time.time()

    conn = None
    cur = None

    try:
        # Verbindung zu Trino herstellen
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )

        # Cursor erzeugen
        cur = conn.cursor()

        # SQL ausführen
        cur.execute(sql)

        # Ergebnisse abholen
        rows = cur.fetchall()

        # Spaltennamen extrahieren
        cols = [c[0] for c in cur.description]

        # DataFrame bauen
        df = pd.DataFrame(rows, columns=cols)

        duration = round(time.time() - start, 3)
        logger.info(f"Trino query finished in {duration}s, rows={len(df)}")

        return df
    
    except Exception as e:
        logger.error(f"Trino error: {str(e)}")
        raise HTTPException(500, detail=f"Trino Fehler: {str(e)}")
    
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

def df_to_json(df):
    return df.to_dict(orient="records")
    
def df_to_csv(df) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")

def df_to_xlsx(df) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return buffer.getvalue()

def df_to_parquet(df) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    return buffer.getvalue()

def df_to_protobuf(df) -> bytes:
    buffer = io.BytesIO()

    for _, row in df.iterrows():
        msg = Struct()
        for col, val in row.items():
            msg[col] = "" if pd.isna(val) else str(val)
        buffer.write(msg.SerializeToString())

    return buffer.getvalue()
