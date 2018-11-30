"""__main__

This module is the main module for the API server.

On startup the api is instantiated.

"""
import os

from gobapi.api import get_app

# get the API object and start it up
get_app().run(port=os.getenv("GOB_API_PORT", 8141))
