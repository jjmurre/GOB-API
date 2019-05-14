import typing
import threading

from gobapi import services

ThreadFactory = typing.Callable[[services.Service], threading.Thread]


def start_all_services(
            service_idents: list,
            registry: dict = services.registry,
            factory: ThreadFactory = services.threaded_service
        ) -> typing.List[threading.Thread]:
    return [
        factory(registry[service_ident]())
        for service_ident in service_idents
        if service_ident in registry
    ]
