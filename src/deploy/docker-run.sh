#!/usr/bin/env bash
set -u   # crash on missing env variables
set -e   # stop on any error
set -x

# run gatekeeper

if [[ -n ${HTTP_PROXY:=} ]]; then
  echo "Using $HTTP_PROXY as http proxy for Gatekeeper\n"
  ./keycloak-gatekeeper --config gatekeeper.conf --openid-provider-proxy $HTTP_PROXY 2>&1 | tee /var/log/gatekeeper/gatekeeper.log &
else
  ./keycloak-gatekeeper --config gatekeeper.conf 2>&1 | tee /var/log/gatekeeper/gatekeeper.log &
fi

exec uwsgi
