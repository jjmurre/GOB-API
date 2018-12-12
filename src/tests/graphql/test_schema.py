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

        def ref_to(self, other):
            return {
                "type": "GOB.Reference",
                "ref": other
            }

        def get_collections(self, catalog_name):
            refs_1 = {"attr1" : self.ref_to("catalog:collection3")}
            refs_2 = {"attr1" : self.ref_to("catalog:collection1")}
            refs_3 = {}
            refs_4 = {"attr1" : self.ref_to("catalog:collection2")}
            return {
                "collection1": {
                    "references": refs_1,
                    "attributes": refs_1
                },
                "collection2": {
                    "references": refs_2,
                    "attributes": refs_2
                },
                "collection3": {
                    "references": refs_3,
                    "attributes": refs_3
                },
                "collection4": {
                    "references": refs_4,
                    "attributes": refs_4
                },
            }

    sorted_refs = _get_sorted_references(MockModel())
    # 1 => 3 implies 3 before 1
    assert(sorted_refs.index('catalog:collection3') < sorted_refs.index('catalog:collection1'))
    # 2 => 1 implies 1 before 2
    assert(sorted_refs.index('catalog:collection1') < sorted_refs.index('catalog:collection2'))
    # 4 => 2 implies 2 before 4
    assert(sorted_refs.index('catalog:collection2') < sorted_refs.index('catalog:collection4'))
