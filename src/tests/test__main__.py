import importlib

import api
importlib.reload(api)

import api.api
importlib.reload(api.api)


class MockApp:
    is_running = False

    def run(self):
        self.is_running = True


def test_main(monkeypatch):
    mockApp = MockApp()
    monkeypatch.setattr(api.api, 'get_app', lambda: mockApp)

    from api import __main__
    assert(mockApp.is_running)
