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


import u1db
from u1db import (
    tests, vectorclock
    )
from u1db.backends import inmemory

simple_doc = '{"key": "value"}'

class DatabaseBaseTests(object):

    def create_database(self, machine_id):
        raise NotImplementedError(self.create_database)

    def setUp(self):
        super(DatabaseBaseTests, self).setUp()
        self.c = self.create_database('test')


class InMemoryDatabaseMixin(object):

    def create_database(self, machine_id):
        return inmemory.InMemoryDatabase(machine_id)

class DatabaseTests(DatabaseBaseTests):

    def test_put_doc_allocating_doc_id(self):
        doc_id, new_rev = self.c.put_doc(None, None, simple_doc)
        self.assertNotEqual(None, doc_id)
        self.assertNotEqual(None, new_rev)
        self.assertEqual((new_rev, simple_doc, False), self.c.get_doc(doc_id))

    ## def test_create_doc_allocating_doc_id(self):
    ##     doc_id, new_rev = self.c.create_doc(simple_doc)
    ##     self.assertNotEqual(None, doc_id)
    ##     self.assertNotEqual(None, new_rev)
    ##     self.assertEqual((new_rev, simple_doc, False), self.c.get_doc(doc_id))

    def test_put_doc_creating_initial(self):
        doc_id, new_rev = self.c.put_doc('my_doc_id', None, simple_doc)
        self.assertEqual((new_rev, simple_doc, False),
                         self.c.get_doc('my_doc_id'))

    def test_get_doc_after_put(self):
        doc_id, new_rev = self.c.put_doc('my_doc_id', None, simple_doc)
        self.assertEqual((new_rev, simple_doc, False), self.c.get_doc('my_doc_id'))

    def test_get_doc_nonexisting(self):
        self.assertEqual((None, None, False), self.c.get_doc('non-existing'))

    def test_put_fails_with_bad_old_rev(self):
        doc_id, old_rev = self.c.put_doc('my_doc_id', None, simple_doc)
        new_doc = '{"something": "else"}'
        self.assertRaises(u1db.InvalidDocRev,
            self.c.put_doc, 'my_doc_id', 'other:1', new_doc)
        self.assertEqual((old_rev, simple_doc, False),
                         self.c.get_doc('my_doc_id'))

    def test_delete_doc(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual((doc_rev, simple_doc, False), self.c.get_doc(doc_id))
        deleted_rev = self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual((deleted_rev, None, False), self.c.get_doc(doc_id))

    def test_delete_doc_non_existant(self):
        self.assertRaises(KeyError,
            self.c.delete_doc, 'non-existing', 'other:1')

    def test_delete_doc_bad_rev(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual((doc_rev, simple_doc, False), self.c.get_doc(doc_id))
        self.assertRaises(u1db.InvalidDocRev,
            self.c.delete_doc, doc_id, 'other:1')
        self.assertEqual((doc_rev, simple_doc, False), self.c.get_doc(doc_id))

    def test_put_updates_transaction_log(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual((1, set([doc_id])), self.c.whats_changed())

    def test_delete_updates_transaction_log(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        db_rev, _ = self.c.whats_changed()
        self.c.delete_doc(doc_id, doc_rev)
        self.assertEqual((2, set([doc_id])), self.c.whats_changed(db_rev))

    def test_whats_changed_returns_one_id_for_multiple_changes(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.put_doc(doc_id, doc_rev, '{"new": "contents"}')
        self.assertEqual((2, set([doc_id])), self.c.whats_changed())

    def test_get_sync_info(self):
        self.assertEqual(('test', 0, 0), self.c.get_sync_info('other'))

    def test_put_updates_state_info(self):
        self.assertEqual(('test', 0, 0), self.c.get_sync_info('other'))
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual(('test', 1, 0), self.c.get_sync_info('other'))

    def test_put_state_info(self):
        self.assertEqual({}, self.c._other_revs)
        self.c.put_state_info('machine', 10)
        self.assertEqual({'machine': 10}, self.c._other_revs)

class TestInMemoryDatabase(InMemoryDatabaseMixin, DatabaseTests,
                           tests.TestCase):
    pass


class DatabaseIndexTests(DatabaseBaseTests):

    def test_create_index(self):
        self.c.create_index('test-idx', ['name'])
        self.assertEqual(['test-idx'], self.c._indexes.keys())

    def test_create_index_evaluates_it(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual({'value': [doc_id]},
                         self.c._indexes['test-idx']._values)

    def test_create_index_multiple_exact_matches(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        doc2_id, doc2_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc),
                          (doc2_id, doc2_rev, simple_doc)],
                         self.c.get_from_index('test-idx', [('value',)]))

    def test_get_from_index(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
                         self.c.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_unmatched(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([], self.c.get_from_index('test-idx', [('novalue',)]))

    def test_get_from_index_some_matches(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
            self.c.get_from_index('test-idx', [('value',), ('novalue',)]))

    def test_get_from_index_multi(self):
        doc = '{"key": "value", "key2": "value2"}'
        doc_id, doc_rev = self.c.put_doc(None, None, doc)
        self.c.create_index('test-idx', ['key', 'key2'])
        self.assertEqual([(doc_id, doc_rev, doc)],
            self.c.get_from_index('test-idx', [('value', 'value2')]))

    def test_put_adds_to_index(self):
        self.c.create_index('test-idx', ['key'])
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
            self.c.get_from_index('test-idx', [('value',)]))

    def test_put_updates_index(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        _, new_doc_rev = self.c.put_doc(doc_id, doc_rev, new_doc)
        self.assertEqual([],
            self.c.get_from_index('test-idx', [('value',)]))
        self.assertEqual([(doc_id, new_doc_rev, new_doc)],
            self.c.get_from_index('test-idx', [('altval',)]))

    def test_delete_updates_index(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        doc2_id, doc2_rev = self.c.put_doc(None, None, simple_doc)
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
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        new_doc = '{"key": "altval"}'
        result = self.c.sync_exchange([(doc_id, 'machine:1', new_doc)],
                                      'machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual([doc_id], self.c._transaction_log)
        self.assertEqual(([], [(doc_id, doc_rev, simple_doc)], 1), result)

    def test_sync_exchange_ignores_convergence(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        result = self.c.sync_exchange([(doc_id, doc_rev, simple_doc)],
                                      'machine', from_machine_rev=10,
                                      last_known_rev=1)
        self.assertEqual([doc_id], self.c._transaction_log)
        self.assertEqual(([], [], 1), result)

    def test_sync_exchange_returns_new_docs(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        result = self.c.sync_exchange([], 'other-machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual([doc_id], self.c._transaction_log)
        self.assertEqual(([(doc_id, doc_rev, simple_doc)], [], 1), result)

    def test_sync_exchange_getting_newer_docs(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.assertEqual([doc_id], self.c._transaction_log)
        new_doc = '{"key": "altval"}'
        result = self.c.sync_exchange([(doc_id, 'test:1|z:2', new_doc)],
                                      'other-machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual([doc_id, doc_id], self.c._transaction_log)
        self.assertEqual(([], [], 2), result)

    def test_sync_exchange_updates_indexes(self):
        doc_id, doc_rev = self.c.put_doc(None, None, simple_doc)
        self.c.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        other_rev = 'test:1|z:2'
        result = self.c.sync_exchange([(doc_id, other_rev, new_doc)],
                                      'other-machine', from_machine_rev=10,
                                      last_known_rev=0)
        self.assertEqual((other_rev, new_doc, False), self.c.get_doc(doc_id))
        self.assertEqual([(doc_id, other_rev, new_doc)],
                         self.c.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.c.get_from_index('test-idx', [('value',)]))


class TestInMemoryDatabaseIndexes(InMemoryDatabaseMixin, DatabaseIndexTests,
                                  tests.TestCase):
    pass


class DatabaseSyncTests(DatabaseBaseTests):

    def setUp(self):
        super(DatabaseSyncTests, self).setUp()
        self.c1 = self.create_database('test1')
        self.c2 = self.create_database('test2')

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
        doc_id, doc_rev = self.c1.put_doc(None, None, simple_doc)
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
        doc_id, doc_rev = self.c2.put_doc(None, None, simple_doc)
        self.c1.sync(self.c2)
        self.c1.create_index('test-idx', ['key'])
        self.assertEqual((doc_rev, simple_doc, False), self.c1.get_doc(doc_id))
        self.assertEqual(1, self.c1._get_other_machine_rev(self.c2._machine_id))
        self.assertEqual(1, self.c2._get_other_machine_rev(self.c1._machine_id))
        self.assertEqual({'receive': {'docs': [], 'from_id': 'test1',
                                      'from_rev': 0, 'last_known_rev': 0},
                          'return': {'new_docs': [(doc_id, doc_rev)],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.c2._last_exchange_log)
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
                         self.c1.get_from_index('test-idx', [('value',)]))

    def test_sync_ignores_convergence(self):
        doc_id, doc_rev = self.c1.put_doc(None, None, simple_doc)
        self.c3 = self.create_database('test3')
        self.c1.sync(self.c3)
        self.c2.sync(self.c3)
        self.c1.sync(self.c2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.c2._last_exchange_log)

    def test_sync_ignores_superseded(self):
        doc_id, doc_rev = self.c1.put_doc(None, None, simple_doc)
        self.c3 = self.create_database('test3')
        self.c1.sync(self.c3)
        self.c2.sync(self.c3)
        new_doc = '{"key": "altval"}'
        _, doc_rev2 = self.c1.put_doc(doc_id, doc_rev, new_doc)
        self.c2.sync(self.c1)
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test2',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [(doc_id, doc_rev2)],
                                     'conf_docs': [], 'last_rev': 2}},
                         self.c1._last_exchange_log)
        self.assertEqual((doc_rev2, new_doc, False), self.c1.get_doc(doc_id))


    def test_sync_sees_remote_conflicted(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        self.c1.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc)
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
        self.assertEqual([(doc_id, doc2_rev, new_doc)],
                         self.c1.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.c1.get_from_index('test-idx', [('value',)]))

    def test_sync_sees_remote_delete_conflicted(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        self.c1.create_index('test-idx', ['key'])
        self.c1.sync(self.c2)
        doc2_rev = doc1_rev
        new_doc = '{"key": "altval"}'
        doc_id, doc1_rev = self.c1.put_doc(doc_id, doc1_rev, new_doc)
        doc2_rev = self.c2.delete_doc(doc_id, doc2_rev)
        self.assertEqual([doc_id, doc_id], self.c1._transaction_log)
        self.c1.sync(self.c2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc1_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 2, 'last_known_rev': 1},
                          'return': {'new_docs': [],
                                     'conf_docs': [(doc_id, doc2_rev)],
                                     'last_rev': 2}},
                         self.c2._last_exchange_log)
        self.assertEqual([doc_id, doc_id, doc_id], self.c1._transaction_log)
        self.assertEqual((doc2_rev, None, True), self.c1.get_doc(doc_id))
        self.assertEqual((doc2_rev, None, False), self.c2.get_doc(doc_id))
        self.assertEqual([], self.c1.get_from_index('test-idx', [('value',)]))

    def test_sync_local_race_conflicted(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        self.c1.create_index('test-idx', ['key'])
        self.c1.sync(self.c2)
        new_doc1 = '{"key": "localval"}'
        new_doc2 = '{"key": "altval"}'
        _, doc2_rev2 = self.c2.put_doc(doc_id, doc1_rev, new_doc2)
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
        self.assertEqual([(doc_id, doc2_rev2, new_doc2)],
                         self.c1.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.c1.get_from_index('test-idx', [('value',)]))
        self.assertEqual([], self.c1.get_from_index('test-idx', [('localval',)]))

    def test_sync_propagates_deletes(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        self.c1.create_index('test-idx', ['key'])
        self.c1.sync(self.c2)
        self.c2.create_index('test-idx', ['key'])
        self.c3 = self.create_database('test3')
        self.c1.sync(self.c3)
        deleted_rev = self.c1.delete_doc(doc_id, doc1_rev)
        self.c1.sync(self.c2)
        self.assertEqual({'receive': {'docs': [(doc_id, deleted_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 2, 'last_known_rev': 1},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 2}},
                         self.c2._last_exchange_log)
        self.assertEqual((deleted_rev, None, False), self.c1.get_doc(doc_id))
        self.assertEqual((deleted_rev, None, False), self.c2.get_doc(doc_id))
        self.assertEqual([], self.c1.get_from_index('test-idx', [('value',)]))
        self.assertEqual([], self.c2.get_from_index('test-idx', [('value',)]))
        self.c2.sync(self.c3)
        self.assertEqual({'receive': {'docs': [(doc_id, deleted_rev)],
                                      'from_id': 'test2',
                                      'from_rev': 2, 'last_known_rev': 0},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 2}},
                         self.c3._last_exchange_log)
        self.assertEqual((deleted_rev, None, False), self.c3.get_doc(doc_id))

    def test_put_refuses_to_update_conflicted(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc1)
        self.c1.sync(self.c2)
        self.assertEqual((doc2_rev, new_doc1, True), self.c1.get_doc(doc_id))
        new_doc2 = '{"key": "local"}'
        self.assertRaises(u1db.ConflictedDoc,
            self.c1.put_doc, doc_id, doc2_rev, new_doc2)

    def test_delete_refuses_for_conflicted(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc1)
        self.c1.sync(self.c2)
        self.assertEqual((doc2_rev, new_doc1, True), self.c1.get_doc(doc_id))
        self.assertRaises(u1db.ConflictedDoc,
            self.c1.delete_doc, doc_id, doc2_rev)

    def test_get_doc_conflicts_unconflicted(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        self.assertEqual([], self.c1.get_doc_conflicts(doc_id))

    def test_get_doc_conflicts_no_such_id(self):
        self.assertEqual([], self.c1.get_doc_conflicts('doc-id'))

    def test_get_doc_conflicts(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc1)
        self.c1.sync(self.c2)
        self.assertEqual([(doc2_rev, new_doc1),
                          (doc1_rev, simple_doc)],
                         self.c1.get_doc_conflicts(doc_id))

    def test_resolve_doc(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc1)
        self.c1.sync(self.c2)
        self.assertEqual([(doc2_rev, new_doc1),
                          (doc1_rev, simple_doc)],
                         self.c1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.c1.resolve_doc(doc_id, simple_doc,
                                                     [doc2_rev, doc1_rev])
        self.assertFalse(has_conflicts)
        self.assertEqual((new_rev, simple_doc, False), self.c1.get_doc(doc_id))
        self.assertEqual([], self.c1.get_doc_conflicts(doc_id))

    def test_resolve_doc_picks_biggest_vcr(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        doc_id, doc1_rev = self.c1.put_doc(doc_id, doc1_rev, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc1)
        doc_id, doc2_rev = self.c2.put_doc(doc_id, doc2_rev, new_doc1)
        self.c1.sync(self.c2)
        self.assertEqual([(doc2_rev, new_doc1),
                          (doc1_rev, simple_doc)],
                         self.c1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.c1.resolve_doc(doc_id, simple_doc,
                                                     [doc2_rev, doc1_rev])
        self.assertFalse(has_conflicts)
        self.assertEqual((new_rev, simple_doc, False), self.c1.get_doc(doc_id))
        self.assertEqual([], self.c1.get_doc_conflicts(doc_id))
        vcr_1 = vectorclock.VectorClockRev(doc1_rev)
        vcr_2 = vectorclock.VectorClockRev(doc2_rev)
        vcr_new = vectorclock.VectorClockRev(new_rev)
        self.assertTrue(vcr_new.is_newer(vcr_1))
        self.assertTrue(vcr_new.is_newer(vcr_2))

    def test_resolve_doc_partial_not_winning(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc2 = '{"key": "valin2"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc2)
        self.c1.sync(self.c2)
        self.assertEqual([(doc2_rev, new_doc2),
                          (doc1_rev, simple_doc)],
                         self.c1.get_doc_conflicts(doc_id))
        self.c3 = self.create_database('test3')
        new_doc3 = '{"key": "valin3"}'
        doc_id, doc3_rev = self.c3.put_doc(doc_id, None, new_doc3)
        self.c1.sync(self.c3)
        self.assertEqual([(doc3_rev, new_doc3),
                          (doc1_rev, simple_doc),
                          (doc2_rev, new_doc2)],
                         self.c1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.c1.resolve_doc(doc_id, simple_doc,
                                                     [doc2_rev, doc1_rev])
        self.assertTrue(has_conflicts)
        self.assertEqual((doc3_rev, new_doc3, True), self.c1.get_doc(doc_id))
        self.assertEqual([(doc3_rev, new_doc3), (new_rev, simple_doc)],
                         self.c1.get_doc_conflicts(doc_id))

    def test_resolve_doc_partial_winning(self):
        doc_id, doc1_rev = self.c1.put_doc(None, None, simple_doc)
        new_doc2 = '{"key": "valin2"}'
        doc_id, doc2_rev = self.c2.put_doc(doc_id, None, new_doc2)
        self.c1.sync(self.c2)
        self.c3 = self.create_database('test3')
        new_doc3 = '{"key": "valin3"}'
        doc_id, doc3_rev = self.c3.put_doc(doc_id, None, new_doc3)
        self.c1.sync(self.c3)
        self.assertEqual([(doc3_rev, new_doc3),
                          (doc1_rev, simple_doc),
                          (doc2_rev, new_doc2)],
                         self.c1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.c1.resolve_doc(doc_id, simple_doc,
                                                     [doc3_rev, doc1_rev])
        self.assertTrue(has_conflicts)
        self.assertEqual([(new_rev, simple_doc),
                          (doc2_rev, new_doc2)],
                         self.c1.get_doc_conflicts(doc_id))


class TestInMemoryDatabaseSync(InMemoryDatabaseMixin, DatabaseSyncTests,
                               tests.TestCase):
    pass

