"""
Simple utility to be able to start exports to the analysis database from Jenkins

"""
import logging
import os
import json
import requests
import argparse
import re
import time

config = {
    "ANALYSE_DATABASE_USER": None,
    "ANALYSE_DATABASE_PASSWORD": None,
    "ANALYSE_DATABASE_HOST_OVERRIDE": None,
    "ANALYSE_DATABASE_PORT_OVERRIDE": None,
}


for variable in config.keys():
    value = os.getenv(variable)
    if value is None:
        logging.error(f"Environment variable {variable} not set")
    else:
        config[variable] = value
assert None not in [config[variable] for variable in config.keys()], "Missing environment variables"


def dump_catalog(dump_api, catalog_name, collection_name):
    url = f"{dump_api}/{catalog_name}/{collection_name}/"
    data = json.dumps({
        "db": {
            "drivername": "postgres",
            "username": config['ANALYSE_DATABASE_USER'],
            "password": config['ANALYSE_DATABASE_PASSWORD'],
            "host": config['ANALYSE_DATABASE_HOST_OVERRIDE'],
            "port": config['ANALYSE_DATABASE_PORT_OVERRIDE']
        }
    })
    headers = {
        "Content-Type": "application/json"
    }

    start_request = time.time()
    result = requests.post(url=url, data=data, headers=headers, stream=True)

    lastline = ""
    start_line = time.time()
    for line in result.iter_lines(chunk_size=1):
        lastline = line.decode()
        end_line = time.time()
        print(f"{lastline} ({(end_line - start_line):.2f} / {(end_line - start_request):.2f} secs)")
        start_line = time.time()

    end_request = time.time()

    print(f"Elapsed time: {(end_request - start_request):.2f} secs")
    if not re.match(r'Exsport completed', lastline):
        print(f'ERROR: Export {catalog_name}-{collection_name} completed with errors')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Dump GOB collection via API to the analysis database'
    )
    parser.add_argument('dump_api', type=str, help='e.g. https://acc.api.data.amsterdam.nl/gob/dump')
    parser.add_argument('catalog', type=str, help='e.g. nap')
    parser.add_argument('collection', type=str, help='e.g. peilmerken')
    args = parser.parse_args()

    dump_catalog(dump_api=args.dump_api, catalog_name=args.catalog, collection_name=args.collection)
