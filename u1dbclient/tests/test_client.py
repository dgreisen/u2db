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

"""The Client class for U1DB."""


from u1dbclient import (
    client,
    tests,
    )


class TestClient(tests.TestCase):

    def test_create(self):
        c = client.Client()

    def test_has_api_sync(self):
        c = client.Client()
        self.assertNotEqual(None, getattr(c, 'sync', None))

    def test_has_api_whatschanged(self):
        c = client.Client()
        self.assertNotEqual(None, getattr(c, 'whats_changed', None))


class TestInMemoryClientBase(tests.TestCase):

    def setUp(self):
        super(TestInMemoryClientBase, self).setUp()
        self.c = client.InMemoryClient()
        self.doc = '{"key": "value"}'


class TestInMemoryClient(TestInMemoryClientBase):

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

    def test_put_doc_allocating_doc_id(self):
        doc_id, new_rev = self.c.put_doc(None, None, self.doc)
        self.assertNotEqual(None, doc_id)
        self.assertNotEqual(None, new_rev)
        self.assertEqual((new_rev, self.doc, False), self.c.get_doc(doc_id))

    def test_put_doc_creating_initial(self):
        doc_id, new_rev = self.c.put_doc('my_doc_id', None, self.doc)
        self.assertEqual({'my_doc_id': (new_rev, self.doc)},
                         self.c._docs)

    def test_get_doc_after_put(self):
        doc_id, new_rev = self.c.put_doc('my_doc_id', None, self.doc)
        self.assertEqual((new_rev, self.doc, False), self.c.get_doc('my_doc_id'))

    def test_get_doc_nonexisting(self):
        self.assertEqual((None, None, False), self.c.get_doc('non-existing'))

    def test_put_fails_with_bad_old_rev(self):
        doc_id, old_rev = self.c.put_doc('my_doc_id', None, self.doc)
        new_doc = '{"something": "else"}'
        self.assertRaises(client.InvalidDocRev,
            self.c.put_doc, 'my_doc_id', 'other:1', new_doc)
        self.assertEqual((old_rev, self.doc, False),
                         self.c.get_doc('my_doc_id'))

    def test_delete_doc(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.assertEqual((doc_rev, self.doc, False), self.c.get_doc(doc_id))
        self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual((None, None, False), self.c.get_doc(doc_id))

    def test_delete_doc_non_existant(self):
        self.assertRaises(KeyError,
            self.c.delete_doc, 'non-existing', 'other:1')

    def test_delete_doc_bad_rev(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.assertEqual((doc_rev, self.doc, False), self.c.get_doc(doc_id))
        self.assertRaises(client.InvalidDocRev,
            self.c.delete_doc, doc_id, 'other:1')
        self.assertEqual((doc_rev, self.doc, False), self.c.get_doc(doc_id))


class TestInMemoryClientIndexes(TestInMemoryClientBase):

    def test__evaluate_index(self):
        self.assertEqual('value', self.c._evaluate_index(['key'], self.doc))

    def test__evaluate_index_field_None(self):
        self.assertEqual(None, self.c._evaluate_index(['missing'], self.doc))

    def test__evaluate_index_subfield_None(self):
        self.assertEqual(None,
                         self.c._evaluate_index(['key', 'missing'], self.doc))

    def test__evaluate_multi_index(self):
        doc = '{"key": "value", "key2": "value2"}'
        self.assertEqual('value\x01value2',
                         self.c._evaluate_index(['key', 'key2'], doc))

    def test__update_index_ignores_None(self):
        doc = '{"key": "value", "key2": "value2"}'
        index = {}
        self.c._update_index(index, ['nokey'], 'doc-id', self.doc)
        self.assertEqual({}, index)

    def test_create_index(self):
        self.c.create_index('test-idx', ['name'])
        self.assertEqual({'test-idx': ['name']}, self.c._index_definitions)

    def test_create_index_evaluates_it(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual({'test-idx': {'value': doc_id}}, self.c._indexes)

    def test_get_from_index(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, self.doc)],
                         self.c.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_unmatched(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([], self.c.get_from_index('test-idx', [('novalue',)]))

    def test_get_from_index_some_matches(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, self.doc)],
            self.c.get_from_index('test-idx', [('value',), ('novalue',)]))

    def test_get_from_index_multi(self):
        doc = '{"key": "value", "key2": "value2"}'
        doc_id, doc_rev = self.c.put_doc(None, None, doc)
        self.c.create_index('test-idx', ['key', 'key2'])
        self.assertEqual([(doc_id, doc_rev, doc)],
            self.c.get_from_index('test-idx', [('value', 'value2')]))

    def test_put_adds_to_index(self):
        self.c.create_index('test-idx', ['key'])
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.assertEqual([(doc_id, doc_rev, self.doc)],
            self.c.get_from_index('test-idx', [('value',)]))

    def test_put_updates_index(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.c.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        _, new_doc_rev = self.c.put_doc(doc_id, doc_rev, new_doc)
        self.assertEqual([],
            self.c.get_from_index('test-idx', [('value',)]))
        self.assertEqual([(doc_id, new_doc_rev, new_doc)],
            self.c.get_from_index('test-idx', [('altval',)]))

    def test_delete_updates_index(self):
        doc_id, doc_rev = self.c.put_doc(None, None, self.doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, self.doc)],
            self.c.get_from_index('test-idx', [('value',)]))
        self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual([],
            self.c.get_from_index('test-idx', [('value',)]))
