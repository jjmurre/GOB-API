import importlib

import gobapi
importlib.reload(gobapi)

import gobapi.api
importlib.reload(gobapi.api)


class MockApp:
    is_running = False

    def run(self, port):
        self.is_running = True


def test_main(monkeypatch):
    mockApp = MockApp()
    monkeypatch.setattr(gobapi.api, 'get_app', lambda: mockApp)

    from gobapi import __main__
    assert(mockApp.is_running)
