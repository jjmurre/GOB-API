import typing
import threading

from gobapi import services

ThreadFactory = typing.Callable[[services.Service], threading.Thread]


def start_all_services(
            service_idents: list,
            registry: dict = services.registry,
            factory: ThreadFactory = services.threaded_service
        ) -> typing.List[threading.Thread]:
    threads = []
    for service_ident in service_idents:
        if service_ident not in registry:
            continue
        threads.append(
            factory(registry[service_ident]())
        )
    return threads
