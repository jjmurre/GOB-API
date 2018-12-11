from gobapi.graphql.schema import _get_sorted_references


def test_sorted_references_empty_model(monkeypatch):
    class MockModel():
        def get_catalogs(self):
            return {}
    assert(_get_sorted_references(MockModel()) == [])

def test_sorted_references(monkeypatch):
    class MockModel():
        def get_catalogs(self):
            return {"catalog": {}}
        def get_collections(self, catalog_name):
            return {
                "collection1": {
                    "attributes": {
                        "attr1" : {
                            "type": "GOB.Reference",
                            "ref": "catalog:collection3"
                        }
                    }
                },
                "collection2": {
                    "attributes": {
                        "attr1" : {
                            "type": "GOB.Reference",
                            "ref": "catalog:collection1"
                        }
                    }
                },
                "collection3": {
                    "attributes": {
                    }
                },
                "collection4": {
                    "attributes": {
                        "attr1" : {
                            "type": "GOB.Reference",
                            "ref": "catalog:collection2"
                        }
                    }
                },
            }
    sorted_refs = _get_sorted_references(MockModel())
    # 1 => 3 implies 3 before 1
    assert(sorted_refs.index('catalog:collection3') < sorted_refs.index('catalog:collection1'))
    # 2 => 1 implies 1 before 2
    assert(sorted_refs.index('catalog:collection1') < sorted_refs.index('catalog:collection2'))
    # 4 => 2 implies 2 before 4
    assert(sorted_refs.index('catalog:collection2') < sorted_refs.index('catalog:collection4'))
