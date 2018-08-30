"""Config Unit tests

The unit tests for the config module.
As it is a unit test all external dependencies are mocked

"""
import importlib

from api.config import get_gobmodel

def test_gobmodel(monkeypatch):
    # The exact contents of the model will change, so
    assert(not get_gobmodel() == {})
