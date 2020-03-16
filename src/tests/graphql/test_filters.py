import datetime
from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from sqlalchemy.sql.elements import AsBoolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from gobcore.model.sa.gob import Base, FIELD
from gobapi.graphql.filters import FilterConnectionField, get_resolve_attribute, \
    get_resolve_inverse_attribute, get_resolve_json_attribute, \
    get_resolve_attribute_missing_relation, models, RelationQuery, InverseRelationQuery, \
    _get_catalog_collection_name_from_table_name


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

    def set_catalog_collection(self, cat, col):
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


class TestFilters(TestCase):

    def test_resolve_json_attribute(self):
        class Obj:
            someattr = {'a': 'a', 'a_b': 'a_b'}

        a = Obj()

        f = get_resolve_json_attribute('someattr')
        assert(f(a, None) == {'a': 'a', 'aB': 'a_b'})

        a.someattr = None
        assert(f(a, None) == None)

    @patch("gobapi.graphql.filters.GOBModel")
    def test_get_catalog_collection_name_from_table_name(self, mock_model):
        result_cat, result_col = _get_catalog_collection_name_from_table_name('table name')

        self.assertEqual(mock_model.return_value.get_catalog_from_table_name.return_value, result_cat)
        self.assertEqual(mock_model.return_value.get_collection_from_table_name.return_value, result_col)

        mock_model.return_value.get_catalog_from_table_name.assert_called_with('table name')
        mock_model.return_value.get_collection_from_table_name.assert_called_with('table name')

    @patch("gobapi.graphql.filters.RelationQuery")
    def test_get_resolve_attribute(self, mock_relation_query):

        innerfunc = get_resolve_attribute('model', 'attribute')
        result = innerfunc('obj', 'info', kwargje='kwargje')

        self.assertEqual(mock_relation_query.return_value.get_results.return_value, result)
        mock_relation_query.assert_called_with(
            src_object='obj',
            dst_model='model',
            attribute_name='attribute',
            kwargje='kwargje'
        )
        mock_relation_query.return_value.populate_source_info.assert_called_with(
            mock_relation_query.return_value.get_results.return_value
        )

    @patch("gobapi.graphql.filters.InverseRelationQuery")
    def test_get_resolve_inverse_attribute(self, mock_relation_query):

        innerfunc = get_resolve_inverse_attribute('model', 'attribute')
        result = innerfunc('obj', 'info', kwargje='kwargje')

        self.assertEqual(mock_relation_query.return_value.get_results.return_value, result)
        mock_relation_query.assert_called_with(
            src_object='obj',
            dst_model='model',
            attribute_name='attribute',
            kwargje='kwargje'
        )
        mock_relation_query.return_value.populate_source_info.assert_not_called()


class TestRelationQuery(TestCase):

    class MockObj:
        def __init__(self, id, volgnummer):
            self._id = id
            self.volgnummer = volgnummer

    class MockRel:
        bronwaarde = 'bronwaarde'
        begin_geldigheid = None
        eind_geldigheid = None

        def __init__(self, src_id, src_volgnummer, dst_id, dst_volgnummer):
            self.src_id = src_id
            self.src_volgnummer = src_volgnummer
            self.dst_id = dst_id
            self.dst_volgnummer = dst_volgnummer

    def setUp(self) -> None:
        self.relation_query = RelationQuery(
            self.MockObj('id1src', 1),
            self.MockObj('id2dst', 2),
            'the_attribute',
        )

    def test_class_attributes(self):
        self.assertEqual('src', RelationQuery.src_side)
        self.assertEqual('dst', RelationQuery.dst_side)
        self.assertEqual(True, RelationQuery.add_relation_table_columns)

    @patch("gobapi.graphql.filters.filter_deleted")
    @patch("gobapi.graphql.filters.filter_active")
    def test_add_relation_table_filters(self, mock_active, mock_deleted):
        mock_src = type('MockSrc', (), {
            '_id': 'the id',
            'volgnummer': 'the volgnummer',
            '__has_states__': False,
        })()
        mock_relation = type('MockRel', (), {
            'src_or_dst_id': 'the id',
            'src_or_dst_volgnummer': 'nonmatching'
        })
        query = MagicMock()

        rq = RelationQuery(mock_src, 'dst', 'attribute')
        rq.src_side = 'src_or_dst'

        result = rq._add_relation_table_filters(query, mock_relation)

        mock_deleted.assert_called_with(query, mock_relation)
        mock_active.assert_called_with(mock_deleted.return_value, mock_relation)
        mock_active.return_value.filter.assert_called_with(True)  # Check on _id
        self.assertEqual(mock_active.return_value.filter.return_value, result)

        # Check with states, but not active
        mock_active.reset_mock()
        setattr(mock_src, '__has_states__', True)
        rq.kwargs['active'] = False
        result = rq._add_relation_table_filters(query, mock_relation)
        mock_active.assert_not_called()
        mock_deleted.return_value.filter.return_value.filter.assert_called_with(False)  # Check on volgnummer
        self.assertEqual(mock_deleted.return_value.filter.return_value.filter.return_value, result)

    @patch("gobapi.graphql.filters.and_")
    def test_add_dst_table_join(self, mock_and):
        mock_dst = type('MockDst', (), {
            '_id': 'the id',
            'volgnummer': 'the volgnummer',
            '__has_states__': False,
            'otherattr': 'otherval',
            'nullattr': MagicMock(),
        })()
        mock_dst.nullattr.is_.return_value = 'checknull'

        mock_relation = type('MockRel', (), {
            'src_or_dst_id': 'the id',
            'src_or_dst_volgnummer': 'nonmatching'
        })
        query = MagicMock()

        rq = RelationQuery('src', mock_dst, 'attribute')
        rq.dst_side = 'src_or_dst'
        rq.RELAY_ARGS = ['active']
        rq.kwargs = {
            'active': True,
            'nullattr': 'null',
            'otherattr': 'otherval',
        }

        # With dst states False
        result = rq._add_dst_table_join(query, mock_relation)
        mock_and.assert_called_with(
            True,
            'checknull',
            True,
        )
        query.join.assert_called_with(mock_dst, mock_and.return_value, isouter=True)
        self.assertEqual(query.join.return_value, result)
        mock_dst.nullattr.is_.assert_called_with(None)

        # With dst states True
        setattr(mock_dst, '__has_states__', True)
        result = rq._add_dst_table_join(query, mock_relation)
        mock_and.assert_called_with(
            True,
            False,
            'checknull',
            True,
        )
        query.join.assert_called_with(mock_dst, mock_and.return_value, isouter=True)
        self.assertEqual(query.join.return_value, result)
        mock_dst.nullattr.is_.assert_called_with(None)

    def test_add_sort(self):
        mock_asc = MagicMock()
        mock_desc = MagicMock()
        mock_dst = type('MockDst', (), {
            'the_column': type('MockColumn', (), {
                'desc': mock_desc,
                'asc': mock_asc
            })
        })
        rq = RelationQuery('src', mock_dst, 'attribute')

        query = MagicMock()
        res = rq._add_sort(query, ['the_column_asc'])
        self.assertEqual(query.order_by.return_value, res)
        mock_asc.assert_called_once()
        mock_desc.assert_not_called()

        mock_asc.reset_mock()
        res = rq._add_sort(query, ['the_column_desc'])
        mock_desc.assert_called_once()
        mock_asc.assert_not_called()

    @patch("gobapi.graphql.filters.resolve_schema_collection_name", lambda x: tuple(x.split('_')))
    def test_build_query(self):
        class MockDst:
            __tablename__ = 'catalog_collection'
            query = MagicMock()

        class MockRelModel:
            bronwaarde = 'bronwaarde'
            begin_geldigheid = type('Mock', (), {'label': lambda x: x})
            eind_geldigheid = type('Mock', (), {'label': lambda x: x})

        mock_dst = MockDst()
        rq = RelationQuery('src', mock_dst, 'attribute')
        rq.add_relation_table_columns = True
        rq._get_relation_model = MagicMock(return_value=MockRelModel())
        rq._add_relation_table_filters = MagicMock()
        rq._add_dst_table_join = MagicMock()

        result = rq._build_query()
        mock_dst.query.set_catalog_collection.assert_called_with('catalog', 'collection')

        mock_relation_model = rq._get_relation_model.return_value
        mock_dst.query.select_from.assert_called_with(mock_relation_model)
        rq._add_relation_table_filters.assert_called_with(mock_dst.query.select_from.return_value, mock_relation_model)
        rq._add_dst_table_join.assert_called_with(rq._add_relation_table_filters.return_value, mock_relation_model)

        rq._add_dst_table_join.return_value.add_columns.assert_called_with(
            'bronwaarde',
            'begin_geldigheid_relatie',
            'eind_geldigheid_relatie'
        )

        self.assertEqual(rq._add_dst_table_join.return_value.add_columns.return_value, result)

        # Test when add_relation_table_columsn set to False
        rq._add_dst_table_join.return_value.add_columns.reset_mock()
        rq.add_relation_table_columns = False

        result = rq._build_query()
        rq._add_dst_table_join.return_value.add_columns.assert_not_called()
        self.assertEqual(rq._add_dst_table_join.return_value, result)

    @patch("gobapi.graphql.filters.resolve_schema_collection_name", lambda x: tuple(x.split('_')))
    def test_build_query_sort(self):
        class MockDst:
            __tablename__ = 'catalog_collection'
            query = MagicMock()

        class MockRelModel:
            bronwaarde = 'bronwaarde'
            begin_geldigheid = type('Mock', (), {'label': lambda x: x})
            eind_geldigheid = type('Mock', (), {'label': lambda x: x})

        mock_dst = MockDst()
        rq = RelationQuery('src', mock_dst, 'attribute')
        rq.add_relation_table_columns = False
        rq._get_relation_model = MagicMock(return_value=MockRelModel())
        rq._add_relation_table_filters = MagicMock()
        rq._add_dst_table_join = MagicMock()
        rq._add_sort = MagicMock()
        rq.kwargs = {
            'sort': ['column_asc']
        }

        result = rq._build_query()
        rq._add_sort.assert_called_with(rq._add_dst_table_join.return_value, ['column_asc'])
        self.assertEqual(rq._add_sort.return_value, result)

    @patch("gobapi.graphql.filters.models", {'dst_table': 'mocked_table'})
    def test_get_results(self):
        rq = RelationQuery('src', type('DstModel', (), {'__tablename__': 'dst_table'}), 'attribute')
        rq._build_query = MagicMock()
        rq._build_query.return_value.all = lambda: ['a', 'b', 'c']
        rq._flatten_join_query_result = MagicMock(side_effect=lambda x, y: 2*x)

        rq.add_relation_table_columns = False
        self.assertEqual(['a', 'b', 'c'], rq.get_results())

        rq.add_relation_table_columns = True
        self.assertEqual(['aa', 'bb', 'cc'], rq.get_results())
        rq._flatten_join_query_result.assert_has_calls([
            call('a', 'mocked_table'),
            call('b', 'mocked_table'),
            call('c', 'mocked_table'),
        ])

    @patch("gobapi.graphql.filters.models")
    def test_populate_source_infos(self, mock_models):
        mock_table = MagicMock()
        
        class MockSrc:
            def __init__(self, source_values):
                self.the_attribute = source_values

        class MockDst:
            __tablename__ = mock_table
            query = MagicMock()

        mock_models['dst_table'] = mock_table

        # Single Reference
        rq = RelationQuery(MockSrc({'bronwaarde': 'bw1', 'broninfo': {'bron': 'info'}}), MockDst(), 'the_attribute')
        results = [
            type('ResultObj', (), {'bronwaarde': 'bw1', 'some_attribute': 'attr1'})
        ]

        rq.populate_source_info(results)
        self.assertEqual({'bron': 'info'}, getattr(results[0], 'broninfo'))

        # Single Reference no result, with source values
        rq = RelationQuery(MockSrc({'bronwaarde': 'bw1', 'broninfo': {'bron': 'info'}}), MockDst(), 'the_attribute')
        results = []
        
        rq.populate_source_info(results)
        self.assertEqual('bw1', getattr(results[0], 'bronwaarde'))

        # ManyReference
        rq = RelationQuery(MockSrc([{'bronwaarde': 'bw1', 'broninfo': {'bron': 'info'}}]), MockDst(), 'the_attribute')
        results = [
            type('ResultObj', (), {'bronwaarde': 'bw1', 'some_attribute': 'attr1'}),
            type('ResultObj', (), {'bronwaarde': 'bw2', 'some_attribute': 'attr2'})
        ]

        rq.populate_source_info(results)
        self.assertEqual({'bron': 'info'}, getattr(results[0], 'broninfo'))
        self.assertEqual(None, getattr(results[1], 'broninfo'))

        # ManyReference no result, with source values
        rq = RelationQuery(MockSrc([{'bronwaarde': 'bw1', 'broninfo': {'bron': 'info'}}]), MockDst(), 'the_attribute')
        results = []
        
        rq.populate_source_info(results)
        self.assertEqual('bw1', getattr(results[0], 'bronwaarde'))


        # Missing attr should pass without problems
        rq = RelationQuery(MockSrc(None), MockDst(), 'the_attribute')
        results = [
            type('ResultObj', (), {'bronwaarde': 'bw1', 'some_attribute': 'attr1'}),
        ]
        rq.populate_source_info(results)

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

        mock_result = MockKeyedTuple((mock_base, 'value1', 'value2'), ['reference', 'variable1', 'variable2'])
        result = self.relation_query._flatten_join_query_result(mock_result, None)

        # Expect the variables to be set as attributes of the mock_base
        self.assertEqual(mock_base.variable1, 'value1')
        self.assertEqual(mock_base.variable2, 'value2')

        # Should create a new base object
        mock_result = MockKeyedTuple((None, 'value1', 'value2'), ['reference', 'variable1', 'variable2'])
        result = self.relation_query._flatten_join_query_result(mock_result, type(mock_base))

        self.assertEqual(result.variable1, 'value1')
        self.assertEqual(result.variable2, 'value2')

    @patch('gobapi.graphql.filters.gobmodel')
    @patch('gobapi.graphql.filters.get_relation_name', lambda m, cat, col, rel: f'{cat}_{col}_{rel}')
    @patch('gobapi.graphql.filters._get_catalog_collection_name_from_table_name', lambda x: tuple(x.split('_')))
    def test_get_relation_model(self, mock_gobmodel):
        class MockObj:
            def __init__(self, tablename):
                self.__tablename__ = tablename

        rq = RelationQuery(MockObj('src_table'), MockObj('dst_table'), 'attribute')

        with patch.dict(models, {'rel_src_table_attribute': 'src model', 'rel_dst_table_attribute': 'dst model'},
                        clear=True):

            self.assertEqual('src model', rq._get_relation_model())

            # Check if src side is dst model
            rq.src_side = 'dst'
            self.assertEqual('dst model', rq._get_relation_model())


class TestInverseRelationQuery(TestCase):

    def test_class_attributes(self):
        self.assertEqual('dst', InverseRelationQuery.src_side)
        self.assertEqual('src', InverseRelationQuery.dst_side)
        self.assertEqual(False, InverseRelationQuery.add_relation_table_columns)
