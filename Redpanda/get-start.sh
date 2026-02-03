#!/bin/bash
docker compose up -d nifi
docker cp  $(docker compose ps -q nifi):/opt/nifi/scripts/start.sh ./start.sh
