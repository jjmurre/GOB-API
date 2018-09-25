"""__main__

This module is the main module for the API server.

On startup the api is instantiated.

"""
from gobapi.api import get_app

# get the API object and start it up
get_app().run()
