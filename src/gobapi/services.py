""" Interface code to external services we run next to the API

Concepts:

 * Service: an implementation to start and stop a certain service
 * threaded service: runs a service in a separate thread
 * teardown function: callable that stops a service and joins the thread
 * teardown adapter: registers a given teardown function in the adapted backend (eg. signal or atexit)

"""

import abc
import atexit
import threading
import typing
import signal as signallib

from gobcore.message_broker import messagedriven_service

from gobapi.logger import get_logger

logger = get_logger("gopapi.services")

TeardownFunc = typing.Callable[[], None]
TeardownAdapter = typing.Callable[[TeardownFunc], None]
AdapterList = typing.List[TeardownAdapter]


def _create_teardown_adapter(adapter_func, *args, **kwargs) -> TeardownAdapter:
    """ Factory function for teardown adapters.
        Restricts how adapters can be used.
    """
    return lambda teardown_func: adapter_func(teardown_func, *args, **kwargs)


def _signal_adapter(
            func: TeardownFunc,
            backend=signallib, term_signal=signallib.SIGINT):
    prev_handler = backend.getsignal(term_signal)
    """ Adapter for registering a teardown func in signal """

    def _sig_handler(emitted_signal, frame):
        func()
        prev_handler(emitted_signal, frame)

    try:
        backend.signal(term_signal, _sig_handler)
    except ValueError:
        pass


def _atexit_adapter(func: TeardownFunc, backend=atexit):
    """ Adapter for registering a teardown func in atexit """
    backend.register(func)


# Registry of publicly availabe teardown adapters
SignalAdapter = _create_teardown_adapter(_signal_adapter)
AtexitAdapter = _create_teardown_adapter(_atexit_adapter)

DEFAULT_TEARDOWN_ADAPTERS = [SignalAdapter, AtexitAdapter]


class Service(abc.ABC):
    name = "Service"
    backend: typing.ClassVar[typing.Callable]

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


def threaded_service(
            service: Service,
            threading_backend=threading.Thread,
            teardown_adapters: AdapterList = DEFAULT_TEARDOWN_ADAPTERS
        ):
    """ Start a threaded service """
    service_thread = threading_backend(
        target=service.start, name=service.name
    )

    def _teardown_func(terminate_timeout=5):
        # default timeout is 5 since polling period in services is 5
        logger.info(f"Stopping service {service.name}")
        service.stop()
        service_thread.join(terminate_timeout)

    for adapter in teardown_adapters:
        adapter(_teardown_func)

    service_thread.start()
    return service_thread
