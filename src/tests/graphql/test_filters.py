import datetime

from graphene_sqlalchemy import SQLAlchemyConnectionField
from gobcore.typesystem.gob_secure_types import SecureString
from gobapi.graphql.filters import _build_query, FilterConnectionField, get_resolve_attribute, get_resolve_secure_attribute
from gobapi import storage


class Session():
    def __init__(self):
        pass

    def query(self, _):
        return Query()


class Columns():
    def __init__(self):
        self.dst_id = "1"
        self.dst_volgnummer = "1"

class Query():
    def __init__(self):
        self.expr = ""
        self.c = Columns()

    def filter(self, expr, *args):
        self.expr = self.expr + str(expr)
        return self

    def filter_by(self, **kwargs):
        return self

    def query(self, _):
        return self

    def subquery(self):
        return self

    def join(self, table, on):
        self.expr = self.expr + "Joined"
        return self

    def all(self):
        return self.expr

class Model():
    def __init__(self, fieldname, value, date_deleted=None):
        setattr(self, fieldname, value)
        self._id = "id"
        self._expiration_date = datetime.datetime.now() + datetime.timedelta(days=1)
        self._date_deleted = date_deleted

    def set_ref(self, ref_name):
        setattr(self, ref_name, {"_id": "id"})


def test_build_query(monkeypatch):
    q = Query()
    q = _build_query(q, Model("field", "anyvalue"), None, field=1)
    assert(q.expr == "False")

    q = Query()
    q = _build_query(q, Model("field", "anyvalue"), None, field="anyvalue")
    assert(q.expr == "True")

    q = Query()
    q = _build_query(q, Model("field", "anyvalue"), None, field="null")
    assert(q.expr == "False")

    q = Query()
    q = _build_query(q, Model("field", None), None, field="null")
    assert(q.expr == "True")

    # Test with a relation model
    rel = Model("src_id", "1")
    setattr(rel, "c", Columns())

    q = Query()
    q = _build_query(q, Model("field", None), rel, field="null")
    assert(q.expr == "TrueJoined")

def test_filterconnectionfield(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())
    q = FilterConnectionField.get_query(Model("field", "anyvalue"), None, field="anyvalue")
    assert(q.expr == "TrueTrue")

    q = FilterConnectionField.get_query(Model("field", "anyvalue", datetime.datetime.now()), None, field="anyvalue")
    assert(q.expr == "FalseTrue")

    q = FilterConnectionField.get_query(Model("field", "anyvalue"), None, field="anyvalue", active=True)
    assert(q.expr == "TruetrueTrue")

def test_resolve_attribute(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())
    monkeypatch.setattr(storage, "session", Session())

    # Setup the relation model
    rel = Model("src_id", "1")
    setattr(rel, "src_volgnummer", "1")

    m = Model("field", "anyvalue")
    setattr(m, 'volgnummer', '1')
    m.set_ref("ref")

    r = get_resolve_attribute(rel, m)
    assert(r(m, None, field=1) == "TrueFalseJoined")
    assert(r(m, None, field="anyvalue") == "TrueTrueJoined")

    m._id = "anotherid"
    assert(r(m, None, field="anyvalue") == "TrueTrueJoined")

    del m.ref["_id"]
    assert(r(m, None, field="anyvalue") == 'TrueTrueJoined')

def test_resolve_secure_attribute(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())
    monkeypatch.setattr(storage, "session", Session())

    # Setup the relation model
    rel = Model("src_id", "1")
    setattr(rel, "src_volgnummer", "1")

    m = Model("field", {
        "i": 0,
        "l": 0,
        "v": "some value"
    })

    r = get_resolve_secure_attribute("field", SecureString)
    assert(r(m, None, field=1) == "**********")
