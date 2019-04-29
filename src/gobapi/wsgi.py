from gobapi.api import get_app
from gobapi import config
from gobapi.infra import start_all_services


start_all_services(config.API_INFRA_SERVICES)

application = get_app()
