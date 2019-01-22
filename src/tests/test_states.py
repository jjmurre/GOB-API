""""States Unit tests

The unit tests for the states module.
As it is a unit test all external dependencies are mocked

"""
import datetime
import importlib


class MockGOBModel:
    def __init__(self):
        self.model = {
            'catalog': {
                'collection': {
                    'fields': {
                        'volgnummer': {'type': 'GOB.String'},
                        'identificatie': {'type': 'GOB.String'},
                        'naam': {'type': 'GOB.String'},
                        'code': {'type': 'GOB.String'},
                    },
                    'references': {
                        'reference': {
                            'ref': 'catalog:collection2',
                            'type': 'GOB.Reference'
                        }
                    }
                },
                'collection2': {
                    'fields': {
                        'volgnummer': {'type': 'GOB.String'},
                        'identificatie': {'type': 'GOB.String'},
                        'naam': {'type': 'GOB.String'},
                        'code': {'type': 'GOB.String'},
                    },
                    'references': {}
                }
            }
        }
        pass

    def get_collection(self, catalog_name, collection_name):
        return self.model[catalog_name][collection_name]


class MockState:
    def __init__(self, kwargs={}):
        for key, value in kwargs.items():
            setattr(self, key, value)


collections = [['catalog', 'collection'], ['catalog', 'collection2']]

collection_name = 'catalog:collection'

relations = {
    'catalog:collection': {
        'reference': 'catalog:collection2'
    },
    'catalog:collection2': {}
}

states_collection = [
    MockState({
        'volgnummer': 1,
        'identificatie': '1',
        'naam': 'Naam',
        'code': 'A',
        'begin_geldigheid': datetime.date(2010, 1, 1),
        'eind_geldigheid': datetime.date(2011, 1, 1),
        'reference': {'id': '1'}
    }),
    MockState({
        'volgnummer': 2,
        'identificatie': '1',
        'naam': 'Naam',
        'code': 'B',
        'begin_geldigheid': datetime.date(2011, 1, 1),
        'eind_geldigheid': datetime.date(2013, 1, 1),
        'reference': {'id': '1'}
    }),
    MockState({
        'volgnummer': 3,
        'identificatie': '1',
        'naam': 'Naam',
        'code': 'C',
        'begin_geldigheid': datetime.date(2013, 1, 1),
        'eind_geldigheid': None,
        'reference': {}
    })
]

states_collection2 = [
    MockState({
        'volgnummer': 1,
        'identificatie': '1',
        'naam': 'Naam2',
        'code': 'AA',
        'begin_geldigheid': datetime.date(2010, 1, 1),
        'eind_geldigheid': datetime.date(2012, 1, 1)
    }),
    MockState({
        'volgnummer': 2,
        'identificatie': '1',
        'naam': 'Naam2',
        'code': 'AB',
        'begin_geldigheid': datetime.date(2012, 1, 1),
        'eind_geldigheid': datetime.date(2013, 1, 1)
    })
]

collections_with_state = {
    'catalog:collection': {
        '1': states_collection
    },
    'catalog:collection2': {
        '1': states_collection2
    }
}


def before_each_state_test(monkeypatch):
    global collections_with_state

    import gobcore.model
    import gobapi.storage

    monkeypatch.setattr(gobapi.storage, 'get_collection_states', lambda catalog, collection: collections_with_state[f'{catalog}:{collection}'])
    monkeypatch.setattr(gobcore.model, 'GOBModel', MockGOBModel)

    import gobapi.states
    importlib.reload(gobapi.states)


def test_get_valid_states_in_timeslot(monkeypatch):
    before_each_state_test(monkeypatch)
    global relations, collections_with_state

    from gobapi.states import _get_valid_states_in_timeslot

    # Test invalid timeslot
    timeslot_start = datetime.date(2009, 1, 1)
    timeslot_end = datetime.date(2010, 1, 1)

    entity_id = '1'

    result = _get_valid_states_in_timeslot(timeslot_start, timeslot_end, collection_name,
                                     entity_id, relations, collections_with_state)
    assert(result == {'catalog:collection': None})

    # Test timeslot with relation
    timeslot_start = datetime.date(2010, 1, 1)
    timeslot_end = datetime.date(2010, 6, 1)
    result = _get_valid_states_in_timeslot(timeslot_start, timeslot_end, collection_name,
                                     entity_id, relations, collections_with_state)

    assert(result == {
        'catalog:collection': states_collection[0],
        'catalog:collection2': states_collection2[0]
    })

    # Test timeslot without relation
    timeslot_start = datetime.date(2013, 1, 1)
    timeslot_end = datetime.date(2014, 1, 1)
    result = _get_valid_states_in_timeslot(timeslot_start, timeslot_end, collection_name,
                                     entity_id, relations, collections_with_state)

    assert(result == {
        'catalog:collection': states_collection[2]
    })


def test_calculate_timeslots_for_entity(monkeypatch):
    before_each_state_test(monkeypatch)
    global relations, collections_with_state, states_collection

    from gobapi.states import _calculate_timeslots_for_entity

    result = _calculate_timeslots_for_entity(states_collection, relations,
                                             collection_name, collections_with_state)
    assert(result == [
        datetime.date(2010,1,1),
        datetime.date(2011,1,1),
        datetime.date(2012,1,1),
        datetime.date(2013,1,1)
    ])


def test_find_relations(monkeypatch):
    before_each_state_test(monkeypatch)

    global collections, relations

    from gobapi.states import _find_relations

    result = _find_relations(collections)
    assert(result == relations)


def test_get_states(monkeypatch):
    before_each_state_test(monkeypatch)

    global collections, relations

    from gobapi.states import get_states

    offset = 0
    limit = 100
    result = get_states(collections, offset, limit)

    # Make sure 4 records are returned
    assert(len(result[0]) == 4)
    assert(result[1] == 4)

    expected_outcome = [
        {'volgnummer': '1', 'identificatie': '1', 'naam': 'Naam', 'code': 'A', 'begin_tijdvak': datetime.date(2010,1,1), 'einde_tijdvak': datetime.date(2011,1,1), 'catalog:collection2_volgnummer': '1', 'catalog:collection2_identificatie': '1', 'catalog:collection2_naam': 'Naam2', 'catalog:collection2_code': 'AA'},
        {'volgnummer': '2', 'identificatie': '1', 'naam': 'Naam', 'code': 'B', 'begin_tijdvak': datetime.date(2011,1,1), 'einde_tijdvak': datetime.date(2012,1,1), 'catalog:collection2_volgnummer': '1', 'catalog:collection2_identificatie': '1', 'catalog:collection2_naam': 'Naam2', 'catalog:collection2_code': 'AA'},
        {'volgnummer': '2', 'identificatie': '1', 'naam': 'Naam', 'code': 'B', 'begin_tijdvak': datetime.date(2012,1,1), 'einde_tijdvak': datetime.date(2013,1,1), 'catalog:collection2_volgnummer': '2', 'catalog:collection2_identificatie': '1', 'catalog:collection2_naam': 'Naam2', 'catalog:collection2_code': 'AB'},
        {'volgnummer': '3', 'identificatie': '1', 'naam': 'Naam', 'code': 'C', 'begin_tijdvak': datetime.date(2013,1,1), 'einde_tijdvak': None, 'catalog:collection2_volgnummer': None, 'catalog:collection2_identificatie': None, 'catalog:collection2_naam': None, 'catalog:collection2_code': None}
    ]

    # See if result matches expected outcome
    for index, row in enumerate(result[0]):
        for key, value in row.items():
            print(index, key, value)
            assert(str(value) == str(expected_outcome[index][key]))

    # Test offset
    offset = 0
    limit = 1
    result = get_states(collections, offset, limit)

    # Make sure 1 record is returned and total count is 4
    assert(len(result[0]) == 1)
    assert(result[1] == 4)
