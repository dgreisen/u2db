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


simple_doc = '{"key": "value"}'

class TestInMemoryClientBase(tests.TestCase):

    def setUp(self):
        super(TestInMemoryClientBase, self).setUp()
        self.c = client.InMemoryClient('test')


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

    def test__get_current_rev_missing(self):
        self.assertEqual(None, self.c._get_current_rev('doc-id'))

    def test__get_current_rev_exists(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual(doc_rev, self.c._get_current_rev(doc_id))

    def test_put_doc_allocating_doc_id(self):
        doc_id, new_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertNotEqual(None, doc_id)
        self.assertNotEqual(None, new_rev)
        self.assertEqual((new_rev, simple_doc, False), self.c.get_doc(doc_id))

    def test_put_doc_creating_initial(self):
        doc_id, new_rev, db_rev = self.c.put_doc('my_doc_id', None, simple_doc)
        self.assertEqual((new_rev, simple_doc, False),
                         self.c.get_doc('my_doc_id'))

    def test_get_doc_after_put(self):
        doc_id, new_rev, db_rev = self.c.put_doc('my_doc_id', None, simple_doc)
        self.assertEqual((new_rev, simple_doc, False), self.c.get_doc('my_doc_id'))

    def test_get_doc_nonexisting(self):
        self.assertEqual((None, None, False), self.c.get_doc('non-existing'))

    def test_put_fails_with_bad_old_rev(self):
        doc_id, old_rev, db_rev = self.c.put_doc('my_doc_id', None, simple_doc)
        new_doc = '{"something": "else"}'
        self.assertRaises(client.InvalidDocRev,
            self.c.put_doc, 'my_doc_id', 'other:1', new_doc)
        self.assertEqual((old_rev, simple_doc, False),
                         self.c.get_doc('my_doc_id'))

    def test_delete_doc(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual((doc_rev, simple_doc, False), self.c.get_doc(doc_id))
        self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual((None, None, False), self.c.get_doc(doc_id))

    def test_delete_doc_non_existant(self):
        self.assertRaises(KeyError,
            self.c.delete_doc, 'non-existing', 'other:1')

    def test_delete_doc_bad_rev(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual((doc_rev, simple_doc, False), self.c.get_doc(doc_id))
        self.assertRaises(client.InvalidDocRev,
            self.c.delete_doc, doc_id, 'other:1')
        self.assertEqual((doc_rev, simple_doc, False), self.c.get_doc(doc_id))

    def test_put_updates_transaction_log(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual(set([doc_id]), self.c.whats_changed(0))

    def test_delete_updates_transaction_log(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual(set([doc_id]), self.c.whats_changed(db_rev))

    def test_whats_changed_returns_one_id_for_multiple_changes(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.put_doc(doc_id, doc_rev, '{"new": "contents"}')
        self.assertEqual(set([doc_id]), self.c.whats_changed(0))

    def test_get_sync_info(self):
        self.assertEqual(('test', 0, 0), self.c.get_sync_info('other'))

    def test_put_updates_state_info(self):
        self.assertEqual(('test', 0, 0), self.c.get_sync_info('other'))
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual(('test', 1, 0), self.c.get_sync_info('other'))

    def test_put_state_info(self):
        self.assertEqual({}, self.c._other_revs)
        self.c.put_state_info('machine', 10)
        self.assertEqual({'machine': 10}, self.c._other_revs)


class TestInMemoryClientIndexes(TestInMemoryClientBase):

    def test_create_index(self):
        self.c.create_index('test-idx', ['name'])
        self.assertEqual(['test-idx'], self.c._indexes.keys())

    def test_create_index_evaluates_it(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual({'value': [doc_id]},
                         self.c._indexes['test-idx']._values)

    def test_create_index_multiple_exact_matches(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        doc2_id, doc2_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc),
                          (doc2_id, doc2_rev, simple_doc)],
                         self.c.get_from_index('test-idx', [('value',)]))

    def test_get_from_index(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
                         self.c.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_unmatched(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([], self.c.get_from_index('test-idx', [('novalue',)]))

    def test_get_from_index_some_matches(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
            self.c.get_from_index('test-idx', [('value',), ('novalue',)]))

    def test_get_from_index_multi(self):
        doc = '{"key": "value", "key2": "value2"}'
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, doc)
        self.c.create_index('test-idx', ['key', 'key2'])
        self.assertEqual([(doc_id, doc_rev, doc)],
            self.c.get_from_index('test-idx', [('value', 'value2')]))

    def test_put_adds_to_index(self):
        self.c.create_index('test-idx', ['key'])
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
            self.c.get_from_index('test-idx', [('value',)]))

    def test_put_updates_index(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        _, new_doc_rev, db_rev = self.c.put_doc(doc_id, doc_rev, new_doc)
        self.assertEqual([],
            self.c.get_from_index('test-idx', [('value',)]))
        self.assertEqual([(doc_id, new_doc_rev, new_doc)],
            self.c.get_from_index('test-idx', [('altval',)]))

    def test_delete_updates_index(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        doc2_id, doc2_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc),
                          (doc2_id, doc2_rev, simple_doc)],
            self.c.get_from_index('test-idx', [('value',)]))
        self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual([(doc2_id, doc2_rev, simple_doc)],
            self.c.get_from_index('test-idx', [('value',)]))

    def test_delete_index(self):
        self.c.create_index('test-idx', ['key'])
        self.assertEqual(['test-idx'], self.c._indexes.keys())
        self.c.delete_index('test-idx')
        self.assertEqual([], self.c._indexes.keys())

    def test_sync_exchange(self):
        result = self.c.sync_exchange([('doc-id', 'machine:1', simple_doc)],
                                      'machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual(('machine:1', simple_doc, False),
                         self.c.get_doc('doc-id'))
        self.assertEqual(['doc-id'], self.c._transaction_log)
        self.assertEqual(([], [], 1), result)
        self.assertEqual(10, self.c.get_sync_info('machine')[-1])

    def test_sync_exchange_refuses_conflicts(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        new_doc = '{"key": "altval"}'
        result = self.c.sync_exchange([(doc_id, 'machine:1', new_doc)],
                                      'machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual([doc_id], self.c._transaction_log)
        self.assertEqual(([], [(doc_id, doc_rev, simple_doc)], 1), result)

    def test_sync_exchange_ignores_convergence(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        result = self.c.sync_exchange([(doc_id, doc_rev, simple_doc)],
                                      'machine', from_machine_rev=10,
                                      last_known_rev=1)
        self.assertEqual([doc_id], self.c._transaction_log)
        self.assertEqual(([], [], 1), result)

    def test_sync_exchange_returns_new_docs(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        result = self.c.sync_exchange([], 'other-machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual([doc_id], self.c._transaction_log)
        self.assertEqual(([(doc_id, doc_rev, simple_doc)], [], 1), result)

    def test_sync_exchange_getting_newer_docs(self):
        doc_id, doc_rev, db_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        new_doc = '{"key": "altval"}'
        result = self.c.sync_exchange([(doc_id, 'test:1|z:2', new_doc)],
                                      'other-machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual([doc_id, doc_id], self.c._transaction_log)
        self.assertEqual(([], [], 2), result)


class TestInMemoryClientSync(tests.TestCase):

    def setUp(self):
        super(TestInMemoryClientSync, self).setUp()
        self.c1 = client.InMemoryClient('test1')
        self.c2 = client.InMemoryClient('test2')

    def test_sync_tracks_db_rev_of_other(self):
        self.c1.sync(self.c2)
        self.assertEqual(0, self.c1._get_other_machine_rev(self.c2._machine_id))
        self.assertEqual(0, self.c2._get_other_machine_rev(self.c1._machine_id))
        self.assertEqual({'receive': {'docs': [], 'from_id': 'test1',
                                      'from_rev': 0, 'last_known_rev': 0},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 0}},
                         self.c2._last_exchange_log)

    def test_sync_puts_changes(self):
        doc_id, doc_rev, db_rev = self.c1.put_doc(None, None, simple_doc)
        self.c1.sync(self.c2)
        self.assertEqual((doc_rev, simple_doc, False), self.c2.get_doc(doc_id))
        self.assertEqual(1, self.c1._get_other_machine_rev(self.c2._machine_id))
        self.assertEqual(1, self.c2._get_other_machine_rev(self.c1._machine_id))
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 1}},
                         self.c2._last_exchange_log)

    def test_sync_pulls_changes(self):
        doc_id, doc_rev, db_rev = self.c2.put_doc(None, None, simple_doc)
        self.c1.sync(self.c2)
        self.assertEqual((doc_rev, simple_doc, False), self.c1.get_doc(doc_id))
        self.assertEqual(1, self.c1._get_other_machine_rev(self.c2._machine_id))
        self.assertEqual(1, self.c2._get_other_machine_rev(self.c1._machine_id))
        self.assertEqual({'receive': {'docs': [], 'from_id': 'test1',
                                      'from_rev': 0, 'last_known_rev': 0},
                          'return': {'new_docs': [(doc_id, doc_rev)],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.c2._last_exchange_log)

    def test_sync_ignores_convergence(self):
        doc_id, doc_rev, db_rev = self.c1.put_doc(None, None, simple_doc)
        self.c3 = client.InMemoryClient('test3')
        self.c1.sync(self.c3)
        self.c2.sync(self.c3)
        self.c1.sync(self.c2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.c2._last_exchange_log)

    def test_sync_sees_remote_conflicted(self):
        doc_id, doc1_rev, db1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc = '{"key": "altval"}'
        doc_id, doc2_rev, db2_rev = self.c2.put_doc(doc_id, None, new_doc)
        self.assertEqual([doc_id], self.c1._transaction_log)
        self.c1.sync(self.c2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc1_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [],
                                     'conf_docs': [(doc_id, doc2_rev)],
                                     'last_rev': 1}},
                         self.c2._last_exchange_log)
        self.assertEqual([doc_id, doc_id], self.c1._transaction_log)
        self.assertEqual((doc2_rev, new_doc, True), self.c1.get_doc(doc_id))
        self.assertEqual((doc2_rev, new_doc, False), self.c2.get_doc(doc_id))

    def test_sync_local_race_conflicted(self):
        doc_id, doc1_rev, _ = self.c1.put_doc(None, None, simple_doc)
        self.c1.sync(self.c2)
        new_doc1 = '{"key": "localval"}'
        new_doc2 = '{"key": "altval"}'
        _, doc2_rev2, _ = self.c2.put_doc(doc_id, doc1_rev, new_doc2)
        # Monkey patch so that after the local client has determined recent
        # changes, we get another one, before sync finishes.
        orig_wc = self.c1.whats_changed
        def after_whatschanged(*args, **kwargs):
            val = orig_wc(*args, **kwargs)
            self.c1.put_doc(doc_id, doc1_rev, new_doc1)
            return val
        self.c1.whats_changed = after_whatschanged
        self.c1.sync(self.c2)
        self.assertEqual((doc2_rev2, new_doc2, True), self.c1.get_doc(doc_id))

    def test_put_refuses_to_update_conflicted(self):
        doc_id, doc1_rev, db1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev, db2_rev = self.c2.put_doc(doc_id, None, new_doc1)
        self.c1.sync(self.c2)
        self.assertEqual((doc2_rev, new_doc1, True), self.c1.get_doc(doc_id))
        new_doc2 = '{"key": "local"}'
        self.assertRaises(client.ConflictedDoc,
            self.c1.put_doc, doc_id, doc2_rev, new_doc2)


class TestInMemoryIndex(tests.TestCase):

    def test_has_name_and_definition(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        self.assertEqual('idx-name', idx._name)
        self.assertEqual(['key'], idx._definition)

    def test_evaluate_json(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        self.assertEqual('value', idx.evaluate_json(simple_doc))

    def test_evaluate_json_field_None(self):
        idx = client.InMemoryIndex('idx-name', ['missing'])
        self.assertEqual(None, idx.evaluate_json(simple_doc))

    def test_evaluate_json_subfield_None(self):
        idx = client.InMemoryIndex('idx-name', ['key', 'missing'])
        self.assertEqual(None, idx.evaluate_json(simple_doc))

    def test_evaluate_multi_index(self):
        doc = '{"key": "value", "key2": "value2"}'
        idx = client.InMemoryIndex('idx-name', ['key', 'key2'])
        self.assertEqual('value\x01value2',
                         idx.evaluate_json(doc))

    def test_update_ignores_None(self):
        idx = client.InMemoryIndex('idx-name', ['nokey'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual({}, idx._values)

    def test_update_adds_entry(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual({'value': ['doc-id']}, idx._values)

    def test_remove_json(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual({'value': ['doc-id']}, idx._values)
        idx.remove_json('doc-id', simple_doc)
        self.assertEqual({}, idx._values)

    def test_remove_json_multiple(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        idx.add_json('doc2-id', simple_doc)
        self.assertEqual({'value': ['doc-id', 'doc2-id']}, idx._values)
        idx.remove_json('doc-id', simple_doc)
        self.assertEqual({'value': ['doc2-id']}, idx._values)

    def test_lookup(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        self.assertEqual(['doc-id'], idx.lookup([('value',)]))

    def test_lookup_multi(self):
        idx = client.InMemoryIndex('idx-name', ['key'])
        idx.add_json('doc-id', simple_doc)
        idx.add_json('doc2-id', simple_doc)
        self.assertEqual(['doc-id', 'doc2-id'], idx.lookup([('value',)]))


class TestVectorClockRev(tests.TestCase):

    def assertIsNewer(self, newer_rev, older_rev):
        new_vcr = client.VectorClockRev(newer_rev)
        old_vcr = client.VectorClockRev(older_rev)
        self.assertTrue(new_vcr.is_newer(old_vcr))
        self.assertFalse(old_vcr.is_newer(new_vcr))

    def assertIsConflicted(self, rev_a, rev_b):
        vcr_a = client.VectorClockRev(rev_a)
        vcr_b = client.VectorClockRev(rev_b)
        self.assertFalse(vcr_a.is_newer(vcr_b))
        self.assertFalse(vcr_b.is_newer(vcr_a))

    def test__is_newer_doc_rev(self):
        self.assertIsNewer('test:1', None)
        self.assertIsNewer('test:2', 'test:1')
        self.assertIsNewer('test:1|other:2', 'test:1|other:1')
        self.assertIsNewer('test:1|other:1', 'other:1')
        self.assertIsConflicted('test:1|other:2', 'test:2|other:1')
        self.assertIsConflicted('test:1|other:1', 'other:2')
        self.assertIsConflicted('test:1', 'test:1')

    def test__expand_None(self):
        vcr = client.VectorClockRev(None)
        self.assertEqual({}, vcr._expand())
        vcr = client.VectorClockRev('')
        self.assertEqual({}, vcr._expand())

    def test__expand(self):
        vcr = client.VectorClockRev('test:1')
        self.assertEqual({'test': 1}, vcr._expand())
        vcr = client.VectorClockRev('other:2|test:1')
        self.assertEqual({'other': 2, 'test': 1}, vcr._expand())

    def assertIncrement(self, original, machine_id, after_increment):
        vcr = client.VectorClockRev(original)
        self.assertEqual(after_increment, vcr.increment(machine_id))

    def test_increment(self):
        self.assertIncrement(None, 'test', 'test:1')
        self.assertIncrement('test:1', 'test', 'test:2')
        self.assertIncrement('other:1', 'test', 'other:1|test:1')

