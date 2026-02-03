import base64
import os
from flask import Flask, request, Response
import requests

app = Flask(__name__)

KC_BASE = os.environ["KEYCLOAK_BASE_URL"].rstrip("/")
REALM = os.environ["KEYCLOAK_REALM"]
CLIENT_ID = os.environ["KEYCLOAK_CLIENT_ID"]
CLIENT_SECRET = os.environ["KEYCLOAK_CLIENT_SECRET"]

TOKEN_URL = f"{KC_BASE}/realms/{REALM}/protocol/openid-connect/token"

def unauthorized():
    return Response("Unauthorized", status=401)

@app.get("/auth")
def auth():
    # ClickHouse sends Basic auth header to this endpoint :contentReference[oaicite:1]{index=1}
    hdr = request.headers.get("Authorization", "")
    if not hdr.lower().startswith("basic "):
        return unauthorized()

    try:
        b64 = hdr.split(" ", 1)[1]
        userpass = base64.b64decode(b64).decode("utf-8")
        username, password = userpass.split(":", 1)
    except Exception:
        return unauthorized()

    # Validate credentials against Keycloak using Resource Owner Password grant.
    # In Keycloak, the client must have "Direct Access Grants" enabled.
    data = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": username,
        "password": password,
    }

    r = requests.post(TOKEN_URL, data=data, timeout=3)
    if r.status_code != 200:
        return unauthorized()

    # Optional: You can return JSON with "settings" to apply session settings in ClickHouse :contentReference[oaicite:2]{index=2}
    return Response("OK", status=200)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
