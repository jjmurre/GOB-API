"""Configuration

The API configuration consists of the specification of the storage (GOB_DB)
and the specification of the model (get_gobmodel)

Todo: CATALOGS and get_gobmodel() should be obtained from GOB-Core

"""
import json
import os

GOB_DB = {
    'drivername': 'postgres',
    'username': os.getenv("DATABASE_USER", "gob"),
    'password': os.getenv("DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("DATABASE_HOST_OVERRIDE", "localhost"),
    'port': os.getenv("DATABASE_PORT_OVERRIDE", 5406),
}

CATALOGS = {
    'meetbouten': {
        'description': 'De meetgegevens van bouten in de Amsterdamse panden',
        'collections': [
            'meetbouten',
            'meting'
        ]
    }
}


def get_gobmodel():
    '''GOB model

    Return the GOB model specification

    :return:
    '''
    path = os.path.join(os.path.dirname(__file__), 'gobmodel.json')
    with open(path) as file:
        data = json.load(file)
    return data
