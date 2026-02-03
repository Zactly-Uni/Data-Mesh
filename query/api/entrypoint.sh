#!/bin/sh

echo "Hole Metabase Session Token..."

TOKEN=$(curl -s -X POST "$METABASE_URL/api/session" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"$METABASE_USERNAME\", \"password\": \"$METABASE_PASSWORD\"}" \
  | jq -r '.id')

if [ "$TOKEN" = "null" ] || [ -z "$TOKEN" ]; then
  echo "Fehler: Konnte kein Session Token holen"
  exit 1
fi

export METABASE_KEY="$TOKEN"

echo "Metabase Session Token erfolgreich gesetzt."

# Starte die API
exec uvicorn api:app --host 0.0.0.0 --port 8000
