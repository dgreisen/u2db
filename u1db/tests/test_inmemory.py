# Copyright 2011 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 3, as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Test in-memory backend internals."""

import json

from u1db import (
    errors,
    tests,
    )
from u1db.backends import inmemory


simple_doc = '{"key": "value"}'


class TestInMemoryDatabaseInternals(tests.TestCase):

    def setUp(self):
        super(TestInMemoryDatabaseInternals, self).setUp()
        self.db = inmemory.InMemoryDatabase('test')

    def test__allocate_doc_id(self):
        self.assertEqual('doc-1', self.db._allocate_doc_id())

    def test__allocate_doc_rev_from_None(self):
        self.assertEqual('test:1', self.db._allocate_doc_rev(None))

    def test__allocate_doc_rev_incremental(self):
        self.assertEqual('test:2', self.db._allocate_doc_rev('test:1'))

    def test__allocate_doc_rev_other(self):
        self.assertEqual('replica:1|test:1',
                         self.db._allocate_doc_rev('replica:1'))

    def test__get_replica_uid(self):
        self.assertEqual('test', self.db._replica_uid)


class TestInMemoryIndex(tests.TestCase):

    def test_has_name_and_definition(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        self.assertEqual('idx-name', idx._name)
        self.assertEqual(['key'], idx._definition)

    def test_evaluate_json(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        self.assertEqual(['value'], idx.evaluate_json(simple_doc))

    def test_evaluate_json_field_None(self):
        idx = inmemory.InMemoryIndex('idx-name', ['missing'])
        self.assertEqual(None, idx.evaluate_json(simple_doc))

    def test_evaluate_json_subfield_None(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key', 'missing'])
        self.assertEqual(None, idx.evaluate_json(simple_doc))

    def test_evaluate_multi_index(self):
        doc = '{"key": "value", "key2": "value2"}'
        idx = inmemory.InMemoryIndex('idx-name', ['key', 'key2'])
        self.assertEqual(['value\x01value2'],
                         idx.evaluate_json(doc))

    def test_update_ignores_None(self):
        idx = inmemory.InMemoryIndex('idx-name', ['nokey'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual({}, idx._values)

    def test_update_adds_entry(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual({'value': ['doc-id']}, idx._values)

    def test_remove_json(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual({'value': ['doc-id']}, idx._values)
        idx.remove_json('doc-id', simple_doc)
        self.assertEqual({}, idx._values)

    def test_remove_json_multiple(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        idx.add_json('doc2-id', simple_doc)
        self.assertEqual({'value': ['doc-id', 'doc2-id']}, idx._values)
        idx.remove_json('doc-id', simple_doc)
        self.assertEqual({'value': ['doc2-id']}, idx._values)

    def test_lookup(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual(['doc-id'], idx.lookup([('value',)]))

    def test_lookup_multi(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        idx.add_json('doc2-id', simple_doc)
        self.assertEqual(['doc-id', 'doc2-id'], idx.lookup([('value',)]))

    def test__find_non_wildcards(self):
        idx = inmemory.InMemoryIndex('idx-name', ['k1', 'k2', 'k3'])
        self.assertEqual(-1, idx._find_non_wildcards(('a', 'b', 'c')))
        self.assertEqual(2, idx._find_non_wildcards(('a', 'b', '*')))
        self.assertEqual(3, idx._find_non_wildcards(('a', 'b', 'c*')))
        self.assertEqual(2, idx._find_non_wildcards(('a', 'b*', '*')))
        self.assertEqual(0, idx._find_non_wildcards(('*', '*', '*')))
        self.assertEqual(1, idx._find_non_wildcards(('a*', '*', '*')))
        self.assertRaises(errors.InvalidValueForIndex,
            idx._find_non_wildcards, ('a', 'b'))
        self.assertRaises(errors.InvalidValueForIndex,
            idx._find_non_wildcards, ('a', 'b', 'c', 'd'))
        self.assertRaises(errors.InvalidValueForIndex,
            idx._find_non_wildcards, ('*', 'b', 'c'))


class ExtractFieldTests(tests.TestCase):

    def test_get_value(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo="bar"))
        self.assertEqual("bar", val)

    def test_get_value_None(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=None))
        self.assertEqual(None, val)

    def test_get_value_missing_key(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict())
        self.assertEqual(None, val)

    def test_get_value_subfield(self):
        getter = inmemory.ExtractField('foo.baz')
        val = getter.get(dict(foo=dict(baz="bar")))
        self.assertEqual("bar", val)

    def test_get_value_subfield_missing(self):
        getter = inmemory.ExtractField('foo.baz')
        val = getter.get(dict(foo="bar"))
        self.assertEqual(None, val)

    def test_get_value_dict(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=dict(baz="bar")))
        # XXX: should give None or error?
        self.assertEqual(None, val)

    def test_get_value_list(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=["bar", "zap"]))
        self.assertEqual(["bar", "zap"], val)

    def test_get_value_list_of_dicts(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=[dict(zap="bar")]))
        self.assertEqual(None, val)

    def test_get_value_int(self):
        # XXX: is the API defined to store and return numbers as
        # numbers as strings?
        # i.e. do we need a "number" transformation?
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=9))
        self.assertEqual(9, val)

    def test_get_value_float(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=9.0))
        self.assertEqual(9.0, val)

    def test_get_value_bool(self):
        getter = inmemory.ExtractField('foo')
        val = getter.get(dict(foo=True))
        self.assertEqual(True, val)


class LowerTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = inmemory.Lower(inmemory.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(None, val)

    def test_inner_returns_string(self):
        getter = inmemory.Lower(inmemory.StaticGetter('fOo'))
        val = getter.get('bar')
        self.assertEqual('foo', val)

    def test_inner_returns_list(self):
        getter = inmemory.Lower(inmemory.StaticGetter(['fOo', 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_int(self):
        getter = inmemory.Lower(inmemory.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_float(self):
        getter = inmemory.Lower(inmemory.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_bool(self):
        getter = inmemory.Lower(inmemory.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_list_containing_int(self):
        getter = inmemory.Lower(inmemory.StaticGetter(['fOo', 9, 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list_containing_float(self):
        getter = inmemory.Lower(inmemory.StaticGetter(['fOo', 9.0, 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list_containing_bool(self):
        getter = inmemory.Lower(inmemory.StaticGetter(['fOo', True, 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)


class SplitWordsTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = inmemory.SplitWords(inmemory.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(None, val)

    def test_inner_returns_string(self):
        getter = inmemory.SplitWords(inmemory.StaticGetter('foo bar'))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list(self):
        getter = inmemory.SplitWords(
            inmemory.StaticGetter(['foo baz', 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_int(self):
        getter = inmemory.SplitWords(inmemory.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_float(self):
        getter = inmemory.SplitWords(inmemory.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_bool(self):
        getter = inmemory.SplitWords(inmemory.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_list_containing_int(self):
        getter = inmemory.SplitWords(
            inmemory.StaticGetter(['foo baz', 9, 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_list_containing_float(self):
        getter = inmemory.SplitWords(
            inmemory.StaticGetter(['foo baz', 9.0, 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_list_containing_bool(self):
        getter = inmemory.SplitWords(
            inmemory.StaticGetter(['foo baz', True, 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)


class IsNullTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = inmemory.IsNull(inmemory.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(True, val)

    def test_inner_returns_string(self):
        getter = inmemory.IsNull(inmemory.StaticGetter('foo'))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_list(self):
        getter = inmemory.IsNull(inmemory.StaticGetter(['foo', 'bar']))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_int(self):
        getter = inmemory.IsNull(inmemory.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_float(self):
        getter = inmemory.IsNull(inmemory.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_bool(self):
        getter = inmemory.IsNull(inmemory.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual(False, val)


class EnsureListTransformationTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = inmemory.EnsureListTransformation(
                inmemory.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(None, val)

    def test_inner_returns_string(self):
        getter = inmemory.EnsureListTransformation(
                inmemory.StaticGetter('foo'))
        val = getter.get('zap')
        self.assertEqual(['foo'], val)

    def test_inner_returns_list(self):
        getter = inmemory.EnsureListTransformation(
                inmemory.StaticGetter(['foo', 'bar']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_int(self):
        getter = inmemory.EnsureListTransformation(
                inmemory.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual([9], val)

    def test_inner_returns_float(self):
        getter = inmemory.EnsureListTransformation(
                inmemory.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual([9.0], val)

    def test_inner_returns_bool(self):
        getter = inmemory.EnsureListTransformation(
                inmemory.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual([True], val)


class ParserTests(tests.TestCase):

    def parse(self, spec):
        parser = inmemory.Parser()
        return parser.parse(spec)

    def parse_all(self, specs):
        parser = inmemory.Parser()
        return parser.parse_all(specs)

    def test_parse_empty_string(self):
        self.assertRaises(inmemory.ParseError, self.parse, "")

    def test_parse_field(self):
        getter = self.parse("a")
        self.assertIsInstance(getter, inmemory.EnsureListTransformation)
        self.assertIsInstance(getter.inner, inmemory.ExtractField)
        self.assertEqual("a", getter.inner.field)

    def test_parse_dotted_field(self):
        getter = self.parse("a.b")
        self.assertIsInstance(getter, inmemory.EnsureListTransformation)
        self.assertIsInstance(getter.inner, inmemory.ExtractField)
        self.assertEqual("a.b", getter.inner.field)

    def test_parse_dotted_field_nothing_after_dot(self):
        self.assertRaises(inmemory.ParseError, self.parse, "a.")

    def test_parse_missing_close_on_transformation(self):
        self.assertRaises(inmemory.ParseError, self.parse, "lower(a")

    def test_parse_missing_field_in_transformation(self):
        self.assertRaises(inmemory.ParseError, self.parse, "lower()")

    def test_parse_transformation(self):
        getter = self.parse("lower(a)")
        self.assertIsInstance(getter, inmemory.EnsureListTransformation)
        self.assertIsInstance(getter.inner, inmemory.Lower)
        self.assertIsInstance(getter.inner.inner, inmemory.ExtractField)
        self.assertEqual("a", getter.inner.inner.field)

    def test_parse_all(self):
        getters = self.parse_all(["a", "b"])
        self.assertEqual(2, len(getters))
        self.assertIsInstance(getters[0], inmemory.EnsureListTransformation)
        self.assertIsInstance(getters[0].inner, inmemory.ExtractField)
        self.assertEqual("a", getters[0].inner.field)
        self.assertIsInstance(getters[1], inmemory.EnsureListTransformation)
        self.assertIsInstance(getters[1].inner, inmemory.ExtractField)
        self.assertEqual("b", getters[1].inner.field)


class IndexTests(tests.TestCase):

    def test_index_lower(self):
        db = inmemory.InMemoryIndex("foo", ["lower(name)"])
        db.add_json("bar", json.dumps(dict(name="Foo")))
        rows = db.lookup([("foo", )])
        self.assertEqual(1, len(rows))
        self.assertEqual("bar", rows[0])

    def test_index_lower_with_lower(self):
        db = inmemory.InMemoryIndex("foo", ["lower(name)"])
        db.add_json("bar", json.dumps(dict(name="foo")))
        rows = db.lookup([("foo", )])
        self.assertEqual(1, len(rows))
        self.assertEqual("bar", rows[0])

    def test_index_lower_doesnt_match(self):
        db = inmemory.InMemoryIndex("foo", ["lower(name)"])
        db.add_json("bar", json.dumps(dict(name="foo")))
        rows = db.lookup([("Foo", )])
        self.assertEqual(0, len(rows))

    def test_index_list(self):
        db = inmemory.InMemoryIndex("foo", ["name"])
        db.add_json("bar", json.dumps(dict(name=["foo", "baz"])))
        rows = db.lookup([("baz", )])
        self.assertEqual(1, len(rows))
        self.assertEqual("bar", rows[0])
