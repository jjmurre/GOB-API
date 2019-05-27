import datetime

from sqlalchemy.sql.elements import AsBoolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from gobcore.typesystem.gob_secure_types import SecureString
from gobapi.graphql.filters import _build_query, _build_query_inverse, FilterConnectionField, get_resolve_attribute, get_resolve_secure_attribute, _add_query_filter_kwargs, get_resolve_inverse_attribute
from gobapi import storage


class Session():
    def __init__(self):
        self.query = Query()

    def query(self, _):
        return self.query


class Columns():
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class Query():
    def __init__(self, c=None):
        self.expr = ""

        if not c:
            self.c = Columns(dst_id="1", dst_volgnummer="1")
        else:
            self.c = c

    def __call__(self, *args, **kwargs):
        return self

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
        if isinstance(on, AsBoolean):
            self.expr = self.expr + "And"
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
        self.__has_states__ = False

    def set_ref(self, ref_name):
        setattr(self, ref_name, {"_id": "id"})

def test_add_query_filter_kwargs():
    q = Query()
    q = _add_query_filter_kwargs(q, Model("field", "anyvalue"), field=1)
    assert(q.expr == "False")

    q = Query()
    q = _add_query_filter_kwargs(q, Model("field", "anyvalue"), field="anyvalue")
    assert(q.expr == "True")

    q = Query()
    q = _add_query_filter_kwargs(q, Model("field", "anyvalue"), field="null")
    assert(q.expr == "False")

    q = Query()
    q = _add_query_filter_kwargs(q, Model("field", None), field="null")
    assert(q.expr == "True")

def test_build_query(monkeypatch):
    # Test with a relation model
    rel = Model("src_id", "1")
    setattr(rel, "c", Columns(dst_id="1", dst_volgnummer="1"))

    q = Query()
    q = _build_query(q, Model("field", None), rel, field="null")
    assert(q.expr == "TrueAndJoined")

def test_build_query_inverse(monkeypatch):
    # Test with a relation model
    rel = Model("dst_id", "1")
    setattr(rel, "c", Columns(src_id="1", src_volgnummer="1"))

    q = Query()
    q = _build_query_inverse(q, Model("field", None), rel, field="null")
    # Should not have "AND" joined
    assert(q.expr == "TrueJoined")

    # Should "AND" join with volgnummer
    model = Model("field", None)
    setattr(model, '__has_states__', True)
    q = Query()
    q = _build_query_inverse(q, model, rel, field="null")
    assert(q.expr == "TrueAndJoined")

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
    assert(r(m, None, field=1) == "TrueFalseAndJoined")
    assert(r(m, None, field="anyvalue") == "TrueTrueAndJoined")

    m._id = "anotherid"
    assert(r(m, None, field="anyvalue") == "TrueTrueAndJoined")

    del m.ref["_id"]
    assert(r(m, None, field="anyvalue") == 'TrueTrueAndJoined')

def test_resolve_attribute_resolve_query(monkeypatch):
    session = Session()

    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: session.query)
    monkeypatch.setattr(storage, "session", session)

    # Setup the relation model
    rel = Model("src_id", "1")
    setattr(rel, "src_volgnummer", "1")

    m = Model("field", "anyvalue")
    setattr(m, 'volgnummer', '1')
    m.set_ref("ref")

    setattr(rel, '_id', '2')
    setattr(m, '__has_states__', True)

    r = get_resolve_attribute(rel, m)

    assert(r(m, None, field="anyvalue") == 'FalseTrueTrueTrueAndJoined')

def test_resolve_inverse_attribute(monkeypatch):
    session = Session()
    q = Query(Columns(src_id="1", src_volgnummer="1"))
    setattr(session, 'query', q)
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: q)
    monkeypatch.setattr(storage, "session", session)

    # Setup the relation model
    rel = Model("dst_id", "1")
    setattr(rel, "dst_volgnummer", "1")

    m = Model("field", "anyvalue")
    setattr(m, 'volgnummer', '1')
    m.set_ref("ref")

    setattr(rel, '_id', '2')
    setattr(m, '__has_states__', True)

    r = get_resolve_inverse_attribute(rel, m)

    assert(r(m, None, field="anyvalue") == 'FalseTrueTrueTrueAndJoined')

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
