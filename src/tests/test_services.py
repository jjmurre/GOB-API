from unittest import mock
import signal as signallib

from gobapi import services


class SignalMock:
    handler = None

    def signal(self, signal, handler):
        if signal == "foo":
            raise ValueError()
        self.handler = handler

    def getsignal(self, signal):
        return lambda s, f: None

    def send(self, signal):
        self.handler(signal, None)


class MockedService:
    running = True
    name = "foo"

    def start(self):
        while self.running:
            pass

    def stop(self):
        self.running = False


def test_signal_adapter():
    mock_signal = SignalMock()
    mock_func = mock.MagicMock()
    services._signal_adapter(mock_func, backend=mock_signal)
    mock_signal.send(signallib.SIGINT)
    mock_func.assert_called_once()
    services._signal_adapter(1, backend=mock_signal, term_signal="foo")
    assert mock_signal != 1


def test_atexit_adapter():
    mock_atexit = mock.MagicMock()
    services._atexit_adapter(lambda: 1, backend=mock_atexit)
    mock_atexit.register.assert_called_once()


def test_threaded_service():
    mock_service = MockedService()
    mock_adapter = mock.MagicMock()

    thread = services.threaded_service(
        mock_service,
        teardown_adapters=[mock_adapter]
    )
    # thread runs immediatly
    assert thread.is_alive()
    # and has the name of the service
    assert thread.name == mock_service.name
    mock_adapter.assert_called()
    mock_adapter.call_args[0][0]()
    # and is terminated after emitting term_signal
    assert not thread.is_alive()


def test_message_driven_service():
    backend = mock.MagicMock()
    service = services.MessageDrivenService(backend=backend)
    service.start()
    assert backend.messagedriven_service.called
    service.stop()
    assert backend.keep_running == False