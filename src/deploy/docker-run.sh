#!/usr/bin/env bash
set -u   # crash on missing env variables
set -e   # stop on any error
set -x

# Run gatekeeper to protect secure endpoints
./keycloak-gatekeeper --config gatekeeper.conf 2>&1 | tee /var/log/gatekeeper/gatekeeper.log &

# Start web server
exec uwsgi
