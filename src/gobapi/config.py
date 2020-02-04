"""Configuration

The API configuration consists of the specification of the storage (GOB_DB)
and the specification of the model (get_gobmodel)

"""
import os

from flask import request
from gobcore.exceptions import GOBException

API_BASE_PATH = '/gob'
API_SECURE_BASE_PATH = '/gob/secure'

GOB_DB = {
    'drivername': 'postgres',
    'username': os.getenv("DATABASE_USER", "gob"),
    'password': os.getenv("DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("DATABASE_HOST_OVERRIDE", "localhost"),
    'port': os.getenv("DATABASE_PORT_OVERRIDE", 5406),
}

# see gobapi.services.registry
API_INFRA_SERVICES = os.getenv(
    "API_INFRA_SERVICES", "MESSAGE_SERVICE"
).upper().split(",")


def current_api_base_path():
    request_base_path_key = 'gob_base_path'

    if not hasattr(request, request_base_path_key):
        paths = sorted([API_BASE_PATH, API_SECURE_BASE_PATH])
        paths.reverse()

        for path in paths:
            if request.path.startswith(path):
                setattr(request, request_base_path_key, path)
                break

    try:
        return getattr(request, request_base_path_key)
    except AttributeError:
        raise GOBException(f"Could not determine base path from {request.path}")
