#!/bin/bash
set -e

sed -i 's|^nifi\.web\.https\.port=.*|nifi.web.https.port=|' conf/nifi.properties
sed -i 's|^nifi\.web\.https\.host=.*|nifi.web.https.host=|' conf/nifi.properties
sed -i 's|^nifi\.web\.http\.port=.*|nifi.web.http.port=8080|' conf/nifi.properties
sed -i 's|^nifi\.web\.http\.host=.*|nifi.web.http.host=0.0.0.0|' conf/nifi.properties

exec bin/nifi.sh run
