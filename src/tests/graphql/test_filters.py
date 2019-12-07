import datetime
import json
from unittest import TestCase
from unittest.mock import patch, MagicMock

from sqlalchemy.sql.elements import AsBoolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from gobcore.typesystem.gob_secure_types import SecureString
from gobcore.model.sa.gob import models, Base
from gobapi.graphql.filters import FilterConnectionField, get_resolve_attribute, get_resolve_auth_attribute, \
    get_resolve_secure_attribute, get_resolve_inverse_attribute, \
    get_resolve_attribute_missing_relation, add_bronwaardes_to_results, gobmodel, _extract_tuples, models, \
    get_fields_in_query, flatten_join_query_result, _extract_relation_model, add_relation_join_query
from gobapi import storage
from gobapi import session

import gobapi.graphql.filters


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
    __tablename__ = "some_tablename"

    def __init__(self, fieldname, value, date_deleted=None):
        setattr(self, fieldname, value)
        self._id = "id"
        self._expiration_date = datetime.datetime.now() + datetime.timedelta(days=1)
        self._date_deleted = date_deleted
        self.__has_states__ = False

    def set_ref(self, ref_name):
        setattr(self, ref_name, {"_id": "id"})


def test_build_query():
    q = Query()
    q = FilterConnectionField._build_query(q, Model("field", "anyvalue"), field=1)
    assert (q.expr == "False")

    q = Query()
    q = FilterConnectionField._build_query(q, Model("field", "anyvalue"), field="anyvalue")
    assert (q.expr == "True")

    q = Query()
    q = FilterConnectionField._build_query(q, Model("field", "anyvalue"), field="null")
    assert (q.expr == "False")

    q = Query()
    q = FilterConnectionField._build_query(q, Model("field", None), field="null")
    assert (q.expr == "True")


def test_filterconnectionfield(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())
    q = FilterConnectionField.get_query(Model("field", "anyvalue"), None, field="anyvalue")
    assert (q.expr == "TrueTrue")

    q = FilterConnectionField.get_query(Model("field", "anyvalue", datetime.datetime.now()), None, field="anyvalue")
    assert (q.expr == "FalseTrue")

    q = FilterConnectionField.get_query(Model("field", "anyvalue"), None, field="anyvalue", active=True)
    assert (q.expr == "TruetrueTrue")


def test_resolve_attribute_missing_relation():
    class Obj:
        someattr = 'somevalue'

    a = Obj()

    f = get_resolve_attribute_missing_relation('someattr')
    assert (f(a, None) == 'somevalue')


def test_add_bronwaardes_to_results(monkeypatch):
    class Obj:
        __tablename__ = "src_table"
        ref = [{'bronwaarde': 'sourceval', 'id': 5, 'broninfo': {'someotherval': 'value'}},
               {'bronwaarde': 'sourceval_missing_relation', 'broninfo': {'someothervalue': 'val'}}]

    src_attr = 'ref'

    model_table_type = type('model_table_name', (), {
        "__tablename__": "model_table_name",
        "__has_states__": False,
        "_id": 5,
        "volgnummer": 3,
        "property": None,
    })

    monkeypatch.setitem(models, 'model_table_name', model_table_type)

    # Case 1. Two bronwaardes, for now we leave bronwaarde empty.
    result = model_table_type()

    results = [result]
    res = add_bronwaardes_to_results(src_attr, model_table_type, Obj(), results)
    assert len(res) == 2
    assert getattr(res[0], 'bronwaarde') == 'sourceval'
    assert getattr(res[1], 'bronwaarde') == 'sourceval_missing_relation'
    assert getattr(res[0], 'broninfo') == {'someotherval': 'value'}
    assert getattr(res[1], 'broninfo') == {'someothervalue': 'val'}

    # Case 2. Single reference, provides bronwaarde as dictionary
    result = model_table_type()
    results = [result]
    obj = Obj()
    obj.__setattr__('ref', {'bronwaarde': 'sourceval', 'broninfo': {'otherkey': 'otherval'}})
    res = add_bronwaardes_to_results(src_attr, model_table_type, obj, results)
    assert len(res) == 1
    assert getattr(res[0], 'bronwaarde') == 'sourceval'
    assert getattr(res[0], 'broninfo') == {'otherkey': 'otherval'}

    # Case 3. Objects have been matched on geometry. Bronwaarde should be geometrie
    result = model_table_type()
    results = [result]
    obj = Obj()
    obj.__setattr__('ref', {'bronwaarde': 'geometrie'})
    res = add_bronwaardes_to_results(src_attr, model_table_type, obj, results)
    assert len(res) == 1
    assert getattr(res[0], 'bronwaarde') == 'geometrie'

    # Case 4. Result has states
    result = model_table_type()
    result.__setattr__('property', 'propval')
    result.__setattr__('__has_states__', True)
    results = [result]
    obj = Obj()
    obj.__setattr__('ref', [
        {'bronwaarde': 'sourceval', 'broninfo': {'otherkey': 'otherval'}},
        {'bronwaarde': 'sourceval2', 'broninfo': {'otherkey': 'matchingobject'}, 'id': 5, 'volgnummer': 3},
        {'bronwaarde': 'sourceval3', 'broninfo': {'otherkey': 'nonmatchingobject'}, 'id': 5, 'volgnummer': 2},
    ])
    res = add_bronwaardes_to_results(src_attr, model_table_type, obj, results)
    assert len(res) == 3
    assert getattr(res[0], 'bronwaarde') == 'sourceval2'
    assert getattr(res[0], 'broninfo') == {'otherkey': 'matchingobject'}
    assert getattr(res[0], 'property') == 'propval'
    assert getattr(res[1], 'bronwaarde') == 'sourceval'
    assert getattr(res[1], 'broninfo') == {'otherkey': 'otherval'}
    assert getattr(res[1], 'property') is None
    assert getattr(res[2], 'bronwaarde') == 'sourceval3'
    assert getattr(res[2], 'broninfo') == {'otherkey': 'nonmatchingobject'}
    assert getattr(res[2], 'property') is None


def test_resolve_secure_attribute(monkeypatch):
    monkeypatch.setattr(SQLAlchemyConnectionField, "get_query", lambda m, i, **kwargs: Query())
    monkeypatch.setattr(storage, "session", Session())
    monkeypatch.setattr(gobapi.graphql.filters, "add_bronwaardes_to_results", lambda s, o, res: res)

    # Setup the relation model
    rel = Model("src_id", "1")
    setattr(rel, "src_volgnummer", "1")

    m = Model("field", json.dumps({
        "i": 0,
        "l": 0,
        "v": "some value"
    }))

    r = get_resolve_secure_attribute("field", SecureString)
    assert (r(m, None, field=1) == "**********")

class TestFilters(TestCase):

    def test_extract_tuples(self):
        input = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}, {'a': 7, 'b': 8}]

        self.assertEqual([(1, 2), (4, 5), (7, 8)], _extract_tuples(input, ('a', 'b')))
        self.assertEqual([(1, 3), (4, 6)], _extract_tuples(input, ('a', 'c')), 'Incomplete tuples should be ignored')
        self.assertEqual([], _extract_tuples(input, ('a', 'd')), 'Incomplete tuples should be ignored')
        self.assertEqual([(1,), (4,), (7,)], _extract_tuples(input, ('a',)), 'Should return single item tuples')
        self.assertEqual([], _extract_tuples([], ('a',)), 'Empty input should result in empty output')

    @patch("gobapi.graphql.filters.get_fields_in_query")
    @patch("gobapi.graphql.filters.FilterConnectionField")
    @patch("gobapi.graphql.filters.add_bronwaardes_to_results")
    @patch("gobapi.graphql.filters._extract_tuples")
    def test_resolve_attribute(self, mock_extract_tuples, mock_add_bronwaardes, mock_filterconnfield,
                               mock_get_fields_in_query):
        class Model():
            _id = MagicMock()
            volgnummer = '2'
            __has_states__ = False

        class Object():
            reference_field = {'bronwaarde': '1', '_id': '1', 'volgnummer': '2'}
            __has_states__ = False

        model = Model()
        obj = Object()
        info = MagicMock()
        kwargs = {'a': '1', 'b': '2'}
        src_attribute_name = 'reference_field'

        resolve_attribute = get_resolve_attribute(model, src_attribute_name)
        result = resolve_attribute(obj, info, **kwargs)

        mock_get_fields_in_query.return_value = []

        mock_filterconnfield.get_query.assert_called_with(model, info, **kwargs)
        get_query_res = mock_filterconnfield.get_query.return_value

        mock_extract_tuples.assert_called_with([obj.reference_field], ('id',))

        get_query_res.filter.assert_called_with(model._id.in_.return_value)
        filter_res = get_query_res.filter.return_value

        filter_res.all.assert_called()

        mock_add_bronwaardes.assert_called_with(src_attribute_name, model, obj, [])

        self.assertEqual(mock_add_bronwaardes.return_value, result)

    @patch("gobapi.graphql.filters.get_fields_in_query")
    @patch("gobapi.graphql.filters.FilterConnectionField")
    @patch("gobapi.graphql.filters.add_bronwaardes_to_results")
    @patch("gobapi.graphql.filters._extract_tuples")
    @patch("gobapi.graphql.filters.tuple_")
    def test_resolve_attribute_with_states(self, mock_tuple_, mock_extract_tuples, mock_add_bronwaardes,
                                           mock_filterconnfield, mock_get_fields_in_query):
        class Model():
            _id = '1'
            volgnummer = '2'
            __has_states__ = True

        class Object():
            reference_field = {'bronwaarde': '1', '_id': '1', 'volgnummer': '2'}
            __has_states__ = True

        model = Model()
        obj = Object()
        info = MagicMock()
        kwargs = {'a': '1', 'b': '2'}
        src_attribute_name = 'reference_field'

        resolve_attribute = get_resolve_attribute(model, src_attribute_name)
        result = resolve_attribute(obj, info, **kwargs)

        mock_get_fields_in_query.return_value = []

        mock_filterconnfield.get_query.assert_called_with(model, info, **kwargs)
        get_query_res = mock_filterconnfield.get_query.return_value

        mock_extract_tuples.assert_called_with([obj.reference_field], ('id', 'volgnummer'))
        mock_tuple_.assert_called_with('1', '2')
        tuple_res = mock_tuple_.return_value

        get_query_res.filter.assert_called_with(tuple_res.in_.return_value)
        filter_res = get_query_res.filter.return_value

        filter_res.all.assert_called()

        mock_add_bronwaardes.assert_called_with(src_attribute_name, model, obj, [])

        self.assertEqual(mock_add_bronwaardes.return_value, result)

    @patch("gobapi.graphql.filters.flatten_join_query_result")
    @patch("gobapi.graphql.filters.add_relation_join_query")
    @patch("gobapi.graphql.filters.get_fields_in_query")
    @patch("gobapi.graphql.filters.FilterConnectionField")
    @patch("gobapi.graphql.filters.add_bronwaardes_to_results")
    @patch("gobapi.graphql.filters._extract_tuples")
    def test_resolve_attribute_join_relation(self, mock_extract_tuples, mock_add_bronwaardes, mock_filterconnfield,
                               mock_get_fields_in_query, mock_add_relation_join_query, mock_flatten_join_query_result):
        class Model():
            _id = MagicMock()
            volgnummer = '2'
            __has_states__ = False

        class Object():
            reference_field = {'bronwaarde': '1', '_id': '1', 'volgnummer': '2'}
            __has_states__ = False

        model = Model()
        obj = Object()
        info = MagicMock()
        kwargs = {'a': '1', 'b': '2'}
        src_attribute_name = 'reference_field'

        mock_get_fields_in_query.return_value = ['beginGeldigheidRelatie']
        relation_res = mock_add_relation_join_query.return_value
        relation_res.all.return_value = ['result']

        resolve_attribute = get_resolve_attribute(model, src_attribute_name)
        result = resolve_attribute(obj, info, **kwargs)

        mock_filterconnfield.get_query.assert_called_with(model, info, **kwargs)
        get_query_res = mock_filterconnfield.get_query.return_value

        mock_extract_tuples.assert_called_with([obj.reference_field], ('id',))

        get_query_res.filter.assert_called_with(model._id.in_.return_value)
        filter_res = get_query_res.filter.return_value

        mock_add_relation_join_query.assert_called_with(obj, model, src_attribute_name, filter_res)

        relation_res.all.assert_called()

        mock_flatten_join_query_result.assert_called_with('result')
        flatten_res = mock_flatten_join_query_result.return_value

        mock_add_bronwaardes.assert_called_with(src_attribute_name, model, obj, [flatten_res])

        self.assertEqual(mock_add_bronwaardes.return_value, result)

    @patch("gobapi.graphql.filters.FilterConnectionField")
    def test_resolve_attribute_inverse(self, mock_filterconnfield):
        class Model():
            reference_field = MagicMock()

        class Object():
            _id = '1'
            volgnummer = '2'
            __has_states__ = False

        model = Model()
        obj = Object()
        info = MagicMock()
        kwargs = {'a': '1', 'b': '2'}
        src_attribute_name = 'reference_field'
        is_many = False

        resolve_attribute = get_resolve_inverse_attribute(model, src_attribute_name, is_many)
        result = resolve_attribute(obj, info, **kwargs)

        mock_filterconnfield.get_query.assert_called_with(model, info, **kwargs)
        get_query_res = mock_filterconnfield.get_query.return_value

        model.reference_field.contains.assert_called_with({'id': '1'})

        get_query_res.filter.assert_called_with(model.reference_field.contains.return_value)
        filter_res = get_query_res.filter.return_value
        self.assertEqual(filter_res.all.return_value, result)

    @patch("gobapi.graphql.filters.FilterConnectionField")
    def test_resolve_attribute_inverse_ismany_and_states(self, mock_filterconnfield):
        class Model():
            reference_field = MagicMock()

        class Object():
            _id = '1'
            volgnummer = '2'
            __has_states__ = True

        model = Model()
        obj = Object()
        info = MagicMock()
        kwargs = {'a': '1', 'b': '2'}
        src_attribute_name = 'reference_field'
        is_many = True

        resolve_attribute = get_resolve_inverse_attribute(model, src_attribute_name, is_many)
        result = resolve_attribute(obj, info, **kwargs)

        mock_filterconnfield.get_query.assert_called_with(model, info, **kwargs)
        get_query_res = mock_filterconnfield.get_query.return_value

        model.reference_field.contains.assert_called_with([{'id': '1', 'volgnummer': '2'}])

        get_query_res.filter.assert_called_with(model.reference_field.contains.return_value)
        filter_res = get_query_res.filter.return_value
        self.assertEqual(filter_res.all.return_value, result)

    @patch("gobapi.graphql.filters.ast_to_dict")
    def test_get_fields_in_query(self, mock_ast_to_dict):
        class ResolveInfo():
            field_asts = ['mock_ast']
            fragments = {}

        mock_ast_to_dict.return_value = {
            'selection_set': {
                'selections': [
                    {'kind': 'Field', 'name': {'value': 'edges'}, 'selection_set': {
                        'selections': [
                            {'kind': 'Field', 'name': {'value': 'edges'}, 'selection_set': {
                                'selections': [
                                    {'kind': 'Field', 'name': {'value': 'field 1'}},
                                    {'kind': 'Field', 'name': {'value': 'field 2'}},
                                    {'kind': 'Other', 'name': {'value': 'field 3'}},
                                ]
                            }}
                        ]
                    }}
                ]
            }
        }

        result = get_fields_in_query(ResolveInfo())

        mock_ast_to_dict.assert_called_with('mock_ast')

        self.assertEqual(result, ['field 1', 'field 2'])

    def test_flatten_join_query_result(self):

        mock_base = Base()

        class MockKeyedTuple(tuple):

            def __new__(cls, vals, labels=None):
                t = tuple.__new__(cls, vals)
                if labels:
                    t.__dict__.update(zip(labels, vals))
                return t

            def _asdict(self):
                return self.__dict__

        mock_result = MockKeyedTuple((mock_base,'value1', 'value2'), ['reference', 'variable1', 'variable2'])
        result = flatten_join_query_result(mock_result)

        # Expect the variables to be set as attributes of the mock_base
        self.assertEqual(mock_base.variable1, 'value1')
        self.assertEqual(mock_base.variable2, 'value2')

    @patch('gobapi.graphql.filters.gobmodel')
    @patch('gobapi.graphql.filters.get_relation_name')
    def test_extract_relation_model(self, mock_get_relation_name, mock_gobmodel):
        class MockObject():

            def __init__(self, table_name):
                self.__tablename__ = table_name

        mock_src_obj = MockObject('cola_cata')
        mock_dst_obj = MockObject('colb_catb')

        mock_get_relation_name.return_value = 'name'

        mock_gobmodel.get_collection_from_table_name.return_value = 'cola'
        mock_gobmodel.get_catalog_from_table_name.return_value = 'cata'

        with patch.dict(models, {'rel_name': 'model'}, clear=True):
            result = _extract_relation_model(mock_src_obj, mock_dst_obj, 'rel')

            mock_get_relation_name.assert_called_with(mock_gobmodel, 'cata', 'cola', 'rel')

            self.assertEqual(result, 'model')

    @patch('gobapi.graphql.filters.and_')
    @patch('gobapi.graphql.filters._extract_relation_model')
    def test_add_relation_join_query(self, mock_extract_relation_model, mock_and):
        class MockObject():
            _id = 'src_id'
            __has_states__ = False

        class MockModel():
            _id = 'src_id'
            __has_states__ = False

        class MockRelationModel():
            src_id = 'src_id'
            dst_id = 'not_dst_id'
            begin_geldigheid = '2000'
            eind_geldigheid = '2010'

        mock_query = MagicMock()

        src_obj = MockObject()
        dst_model = MockModel()

        mock_extract_relation_model.return_value = MockRelationModel()

        result = add_relation_join_query(src_obj, dst_model, 'attribute_name', mock_query)

        mock_extract_relation_model.assert_called_with(src_obj=src_obj, dst_model=dst_model, relation_name='attribute_name')
        mock_relation_model = mock_extract_relation_model.return_value

        mock_and.assert_called_with(*[True, False])

        mock_query.join.assert_called_with(mock_relation_model, mock_and.return_value)
        join_res = mock_query.join.return_value

        join_res.add_columns.assert_called_with('2000', '2010')
        add_columns_res = join_res.add_columns.return_value

        self.assertEqual(result, add_columns_res)

    @patch('gobapi.graphql.filters.and_')
    @patch('gobapi.graphql.filters._extract_relation_model')
    def test_add_relation_join_query_with_states(self, mock_extract_relation_model, mock_and):
        class MockObject():
            _id = 'src_id'
            volgnummer = 'src_volgnummer'
            __has_states__ = True

        class MockModel():
            _id = 'src_id'
            volgnummer = 'dst_volgnummer'
            __has_states__ = True

        class MockRelationModel():
            src_id = 'src_id'
            dst_id = 'not_dst_id'

            src_volgnummer = 'src_volgnummer'
            dst_volgnummer = 'not_dst_volgnummer'

            begin_geldigheid = '2000'
            eind_geldigheid = '2010'

        mock_query = MagicMock()

        src_obj = MockObject()
        dst_model = MockModel()

        mock_extract_relation_model.return_value = MockRelationModel()

        result = add_relation_join_query(src_obj, dst_model, 'attribute_name', mock_query)

        mock_extract_relation_model.assert_called_with(src_obj=src_obj, dst_model=dst_model, relation_name='attribute_name')
        mock_relation_model = mock_extract_relation_model.return_value

        mock_and.assert_called_with(*[True, False, True, False])

        mock_query.join.assert_called_with(mock_relation_model, mock_and.return_value)
        join_res = mock_query.join.return_value

        join_res.add_columns.assert_called_with('2000', '2010')
        add_columns_res = join_res.add_columns.return_value

        self.assertEqual(result, add_columns_res)

    @patch('gobapi.graphql.filters.Authority')
    def test_resolve_auth_attribute(self, mock_authority_class):
        mock_authority = MagicMock()
        mock_authority_class.return_value = mock_authority

        org_resolver = lambda obj, info, **kwargs: "Original resolver"
        mock_authority.get_suppressed_columns = lambda: ['b']

        class MockObject():

            def __init__(self):
                self.a = 'a'
                self.b = 'b'
                self.c = 'c'

        resolver = get_resolve_auth_attribute('cat', 'col', 'b', org_resolver)
        obj = MockObject()
        self.assertEqual(resolver(obj, None), None)

        resolver = get_resolve_auth_attribute('cat', 'col', 'a', org_resolver)
        obj = MockObject()
        self.assertEqual(resolver(obj, None), "Original resolver")

        resolver = get_resolve_auth_attribute('cat', 'col', 'c', None)
        obj = MockObject()
        self.assertEqual(resolver(obj, None), "c")
