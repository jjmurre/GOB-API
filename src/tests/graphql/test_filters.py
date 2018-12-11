from graphene_sqlalchemy import SQLAlchemyConnectionField
from gobapi.graphql.filters import _build_query, FilterConnectionField, get_resolve_attribute


class Query():
    def __init__(self):
        self.expr = ""

    def filter(self, expr):
        self.expr = self.expr + str(expr)
        return self

    def filter_by(self, **kwargs):
        return self

    def query(self, _):
        return self

    def all(self):
        return self.expr

class Model():
    def __init__(self, fieldname, value="anyvalue"):
        setattr(self, fieldname, value)
        self._id = "id"

    def set_ref(self, ref_name):
        setattr(self, ref_name, {"id": "id"})


def test_build_query(monkeypatch):
    q = Query()
    q = _build_query(q, Model("field"), field=1)
    assert(q.expr == "False")

    q = Query()
    q = _build_query(q, Model("field"), field="anyvalue")
    assert(q.expr == "True")

    q = Query()
    q = _build_query(q, Model("field"), field="null")
    assert(q.expr == "False")

    q = Query()
    q = _build_query(q, Model("field", None), field="null")
    assert(q.expr == "True")

def test_filterconnectionfield(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())
    q = FilterConnectionField.get_query(Model("field"), None, field="anyvalue")
    assert(q.expr == "True")

def test_resolve_attribute(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())

    m = Model("field")
    m.set_ref("ref")
    r = get_resolve_attribute(m, "ref")
    assert(r(m, None, field=1) == "TrueFalse")
    assert(r(m, None, field="anyvalue") == "TrueTrue")

    m._id = "anotherid"
    assert(r(m, None, field="anyvalue") == "FalseTrue")
