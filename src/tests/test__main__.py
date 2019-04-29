import importlib
from unittest.mock import patch

import gobapi
importlib.reload(gobapi)

import gobapi.api
importlib.reload(gobapi.api)

import gobapi.config


class MockApp:
    is_running = False

    def run(self, port):
        self.is_running = True


@patch("gobapi.services.registry")
def test_main(MockReg, monkeypatch):
    mockApp = MockApp()
    reg = MockReg()
    monkeypatch.setattr(gobapi.api, 'get_app', lambda: mockApp)

    from gobapi import __main__
    assert(mockApp.is_running)
