"""Configuration

The API configuration consists of the specification of the storage (GOB_DB)
and the specification of the model (get_gobmodel)

"""
import os

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
