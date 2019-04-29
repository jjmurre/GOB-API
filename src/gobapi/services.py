""" Interface code to external services we run next to the API """

import abc
import atexit
import threading
import typing
import signal as signallib

from gobcore.logging.logger import Logger
from gobcore.message_broker import messagedriven_service

logger = Logger("gopapi.services")


def _signal_adapter(
            func, backend=signallib, term_signal=signallib.SIGINT):
    prev_handler = backend.getsignal(term_signal)
    """ Adapter for registering a teardown func in signal """

    def _sig_handler(emitted_signal, frame):
        func()
        prev_handler(emitted_signal, frame)

    try:
        backend.signal(term_signal, _sig_handler)
    except ValueError:
        pass


def _atexit_adapter(func, backend=atexit):
    """ Adapter for registering a teardown func in atexit """
    backend.register(func)


class Service:
    name = "Service"
    backend = None

    def __init__(self, backend=None):
        if backend:
            self.backend = backend

    @abc.abstractmethod
    def start(self):
        """ Starts the service """

    @abc.abstractmethod
    def stop(self):
        """ Stop the service """


class MessageDrivenService(Service):
    """ Interface to gobcore.message_broker.messagedriven_service """
    name = "_MessageService"  # start with underscore then ignored by workflow
    backend = messagedriven_service

    def start(self):
        self.backend.messagedriven_service({}, "API")

    def stop(self):
        self.backend.keep_running = False


registry = {
    'MESSAGE_SERVICE': MessageDrivenService
}

AdapterList = typing.List[typing.Any]

DEFAULT_TEARDOWN_ADAPTERS = [_signal_adapter, _atexit_adapter]


def threaded_service(
            service: Service,
            threading_backend=threading.Thread,
            teardown_adapters: AdapterList = DEFAULT_TEARDOWN_ADAPTERS
        ):
    """ Start a threaded service """
    service_thread = threading_backend(
        target=service.start, name=service.name
    )

    def _exit():
        logger.info(f"Stopping service {service.name}")
        service.stop()
        service_thread.join(4)

    for adapter in teardown_adapters:
        adapter(_exit)

    service_thread.start()
    return service_thread
