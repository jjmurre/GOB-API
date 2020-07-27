"""
login_keycloak.py

Helper script to request a Keycloak token, needed by the API for secure endpoints
"""

import requests
import os
import getpass

KEYCLOAK_AUTH_URL = os.getenv('KEYCLOAK_AUTH_URL')
KEYCLOAK_CLIENT_ID = os.getenv('KEYCLOAK_CLIENT_ID')

if not all([KEYCLOAK_CLIENT_ID, KEYCLOAK_AUTH_URL]):
    print("Missing environment variables. Refer to .env.example in this directory")
    exit(1)

username = input('Username of Keycloak account? ')
password = getpass.getpass('Password of Keycloak account? ')

data = {
    "grant_type": "password",
    "client_id": KEYCLOAK_CLIENT_ID,
    "username": username,
    "password": password,
}

resp = requests.post(KEYCLOAK_AUTH_URL, data=data)
resp.raise_for_status()

jsonresp = resp.json()
auth_header = jsonresp['token_type'] + ' ' + jsonresp['access_token']
print('Token received, please run:')
print(f'export GOB_API_AUTH_HEADER="{auth_header}"')
