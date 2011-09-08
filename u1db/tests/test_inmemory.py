# Copyright (C) 2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""Test in-memory backend internals."""

import u1db
from u1db import (
    tests, vectorclock
    )
from u1db.backends import inmemory


simple_doc = '{"key": "value"}'


class TestInMemoryDatabaseInternals(tests.TestCase):

    def setUp(self):
        super(TestInMemoryDatabaseInternals, self).setUp()
        self.c = inmemory.InMemoryDatabase('test')

    def test__allocate_doc_id(self):
        self.assertEqual('doc-1', self.c._allocate_doc_id())

    def test__allocate_doc_rev_from_None(self):
        self.assertEqual('test:1', self.c._allocate_doc_rev(None))

    def test__allocate_doc_rev_incremental(self):
        self.assertEqual('test:2', self.c._allocate_doc_rev('test:1'))

    def test__allocate_doc_rev_other(self):
        self.assertEqual('machine:1|test:1',
                         self.c._allocate_doc_rev('machine:1'))

    def test__get_machine_id(self):
        self.assertEqual('test', self.c._machine_id)

    def test__get_current_rev_missing(self):
        self.assertEqual(None, self.c._get_current_rev('doc-id'))

    def test__get_current_rev_exists(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual(doc_rev, self.c._get_current_rev(doc_id))


class TestInMemoryIndex(tests.TestCase):

    def test_has_name_and_definition(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        self.assertEqual('idx-name', idx._name)
        self.assertEqual(['key'], idx._definition)

    def test_evaluate_json(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key'])
        self.assertEqual('value', idx.evaluate_json(simple_doc))

    def test_evaluate_json_field_None(self):
        idx = inmemory.InMemoryIndex('idx-name', ['missing'])
        self.assertEqual(None, idx.evaluate_json(simple_doc))

    def test_evaluate_json_subfield_None(self):
        idx = inmemory.InMemoryIndex('idx-name', ['key', 'missing'])
        self.assertEqual(None, idx.evaluate_json(simple_doc))

    def test_evaluate_multi_index(self):
        doc = '{"key": "value", "key2": "value2"}'
        idx = inmemory.InMemoryIndex('idx-name', ['key', 'key2'])
        self.assertEqual('value\x01value2',
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
