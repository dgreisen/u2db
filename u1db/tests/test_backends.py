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

"""The backend class for U1DB. This deals with hiding storage details."""

from u1db import (
    errors,
    tests,
    vectorclock,
    )
from u1db.backends import (
    inmemory,
    sqlite_backend,
    )


simple_doc = '{"key": "value"}'
nested_doc = '{"key": "value", "sub": {"doc": "underneath"}}'


def create_memory_database(machine_id):
    return inmemory.InMemoryDatabase(machine_id)


def create_sqlite_expanded(machine_id):
    db = sqlite_backend.SQLiteExpandedDatabase(':memory:')
    db._set_machine_id(machine_id)
    return db


def create_sqlite_partial_expanded(machine_id):
    db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
    db._set_machine_id(machine_id)
    return db


def create_sqlite_only_expanded(machine_id):
    db = sqlite_backend.SQLiteOnlyExpandedDatabase(':memory:')
    db._set_machine_id(machine_id)
    return db


class DatabaseBaseTests(tests.TestCase):

    create_database = None
    scenarios = [
        ('mem', {'create_database': create_memory_database}),
        ('sql_expand', {'create_database': create_sqlite_expanded}),
        ('sql_partexpand', {'create_database': create_sqlite_partial_expanded}),
        ('sql_onlyexpand', {'create_database': create_sqlite_only_expanded}),
        ]

    def close_database(self, database):
        """Close the database that was opened by create_database.

        The default implementation is a no-op.
        """

    def setUp(self):
        super(DatabaseBaseTests, self).setUp()
        self.db = self.create_database('test')

    def tearDown(self):
        self.close_database(self.db)
        super(DatabaseBaseTests, self).tearDown()


class DatabaseTests(DatabaseBaseTests):

    def test_create_doc_allocating_doc_id(self):
        doc_id, new_rev = self.db.create_doc(simple_doc)
        self.assertNotEqual(None, doc_id)
        self.assertNotEqual(None, new_rev)
        self.assertEqual((new_rev, simple_doc, False), self.db.get_doc(doc_id))

    def test_create_doc_with_id(self):
        doc_id, new_rev = self.db.create_doc(simple_doc, doc_id='my-id')
        self.assertEqual('my-id', doc_id)
        self.assertNotEqual(None, new_rev)
        self.assertEqual((new_rev, simple_doc, False), self.db.get_doc('my-id'))

    def test_create_doc_existing_id(self):
        doc_id, new_rev = self.db.create_doc(simple_doc)
        new_doc = '{"something": "else"}'
        self.assertRaises(errors.InvalidDocRev, self.db.create_doc,
                          new_doc, doc_id)
        self.assertEqual((new_rev, simple_doc, False), self.db.get_doc(doc_id))

    def test_put_doc_refuses_no_id(self):
        self.assertRaises(errors.InvalidDocId,
            self.db.put_doc, None, None, simple_doc)

    def test_get_docs(self):
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        doc2_id, doc2_rev = self.db.create_doc(nested_doc)
        self.assertEqual([(doc1_id, doc1_rev, simple_doc),
                          (doc2_id, doc2_rev, nested_doc)],
                         self.db.get_docs([doc1_id, doc2_id]))

    def test_put_doc_creating_initial(self):
        new_rev = self.db.put_doc('my_doc_id', None, simple_doc)
        self.assertEqual((new_rev, simple_doc, False),
                         self.db.get_doc('my_doc_id'))

    def test_simple_put_docs(self):
        self.assertEqual((set(), set(), 2), self.db.put_docs(
            [('my-doc-id', 'test:1', simple_doc),
             ('my-doc2-id', 'test:1', nested_doc)]))
        self.assertEqual(('test:1', simple_doc, False),
                         self.db.get_doc('my-doc-id'))
        self.assertEqual(('test:1', nested_doc, False),
                         self.db.get_doc('my-doc2-id'))

    def test_put_docs_already_superseded(self):
        orig_doc = '{"new": "doc"}'
        doc1_id, doc1_rev1 = self.db.create_doc(orig_doc)
        doc1_rev2 = self.db.put_doc(doc1_id, doc1_rev1, simple_doc)
        # Nothing is inserted, because the document is already superseded
        self.assertEqual((set(), set([doc1_id]), 0), self.db.put_docs(
            [(doc1_id, doc1_rev1, orig_doc)]))
        self.assertEqual((doc1_rev2, simple_doc, False),
                         self.db.get_doc(doc1_id))

    def test_put_docs_conflicted(self):
        doc1_id, doc1_rev1 = self.db.create_doc(simple_doc)
        # Nothing is inserted, the document id is returned as would-conflict
        self.assertEqual((set([doc1_id]), set(), 0), self.db.put_docs(
            [(doc1_id, 'alternate:1', nested_doc)]))
        # The database wasn't updated yet, either
        self.assertEqual((doc1_rev1, simple_doc, False),
                         self.db.get_doc(doc1_id))

    def test_get_doc_after_put(self):
        doc_id, new_rev = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.assertEqual((new_rev, simple_doc, False),
                         self.db.get_doc('my_doc_id'))

    def test_get_doc_nonexisting(self):
        self.assertEqual((None, None, False), self.db.get_doc('non-existing'))

    def test_get_sync_generation(self):
        self.assertEqual(0, self.db.get_sync_generation('other-db'))
        self.db.set_sync_generation('other-db', 2)
        self.assertEqual(2, self.db.get_sync_generation('other-db'))

    def test_put_fails_with_bad_old_rev(self):
        doc_id, old_rev = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        new_doc = '{"something": "else"}'
        self.assertRaises(errors.InvalidDocRev,
            self.db.put_doc, 'my_doc_id', 'other:1', new_doc)
        self.assertEqual((old_rev, simple_doc, False),
                         self.db.get_doc('my_doc_id'))

    def test_delete_doc(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual((doc_rev, simple_doc, False), self.db.get_doc(doc_id))
        deleted_rev = self.db.delete_doc(doc_id, doc_rev)
        self.assertNotEqual(None, deleted_rev)
        self.assertEqual((deleted_rev, None, False), self.db.get_doc(doc_id))

    def test_delete_doc_non_existant(self):
        self.assertRaises(KeyError,
            self.db.delete_doc, 'non-existing', 'other:1')

    def test_delete_doc_already_deleted(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        new_rev = self.db.delete_doc(doc_id, doc_rev)
        self.assertRaises(KeyError, self.db.delete_doc, doc_id, new_rev)
        self.assertEqual((new_rev, None, False), self.db.get_doc(doc_id))

    def test_delete_doc_bad_rev(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual((doc_rev, simple_doc, False), self.db.get_doc(doc_id))
        self.assertRaises(errors.InvalidDocRev,
            self.db.delete_doc, doc_id, 'other:1')
        self.assertEqual((doc_rev, simple_doc, False), self.db.get_doc(doc_id))

    def test_put_updates_transaction_log(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        doc_rev = self.db.put_doc(doc_id, doc_rev, '{"something": "else"}')
        self.assertEqual([doc_id, doc_id], self.db._get_transaction_log())
        self.assertEqual((2, set([doc_id])), self.db.whats_changed())

    def test_delete_updates_transaction_log(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        db_rev, _ = self.db.whats_changed()
        self.db.delete_doc(doc_id, doc_rev)
        self.assertEqual((2, set([doc_id])), self.db.whats_changed(db_rev))

    def test_whats_changed_initial_database(self):
        self.assertEqual((0, set()), self.db.whats_changed())

    def test_whats_changed_returns_one_id_for_multiple_changes(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.put_doc(doc_id, doc_rev, '{"new": "contents"}')
        self.assertEqual((2, set([doc_id])), self.db.whats_changed())
        self.assertEqual((2, set()), self.db.whats_changed(2))

    def test_handles_nested_content(self):
        doc_id, new_rev = self.db.create_doc(nested_doc)
        self.assertEqual((new_rev, nested_doc, False), self.db.get_doc(doc_id))

    def test_handles_doc_with_null(self):
        doc_id, new_rev = self.db.create_doc('{"key": null}')
        self.assertEqual((new_rev, '{"key": null}', False),
                         self.db.get_doc(doc_id))


class DatabaseIndexTests(DatabaseBaseTests):

    def test_create_index(self):
        self.db.create_index('test-idx', ['name'])
        self.assertEqual([('test-idx', ['name'])],
                         self.db.list_indexes())

    def test_create_index_evaluates_it(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_create_index_multiple_exact_matches(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        doc2_id, doc2_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc),
                          (doc2_id, doc2_rev, simple_doc)],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_get_from_index(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_unmatched(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([], self.db.get_from_index('test-idx', [('novalue',)]))

    def test_get_from_index_some_matches(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
            self.db.get_from_index('test-idx', [('value',), ('novalue',)]))

    def test_get_from_index_multi(self):
        doc = '{"key": "value", "key2": "value2"}'
        doc_id, doc_rev = self.db.create_doc(doc)
        self.db.create_index('test-idx', ['key', 'key2'])
        self.assertEqual([(doc_id, doc_rev, doc)],
            self.db.get_from_index('test-idx', [('value', 'value2')]))

    def test_nested_index(self):
        doc_id, doc_rev = self.db.create_doc(nested_doc)
        self.db.create_index('test-idx', ['sub.doc'])
        self.assertEqual([(doc_id, doc_rev, nested_doc)],
            self.db.get_from_index('test-idx', [('underneath',)]))
        doc2_id, doc2_rev = self.db.create_doc(nested_doc)
        self.assertEqual(
            sorted([(doc_id, doc_rev, nested_doc),
                    (doc2_id, doc2_rev, nested_doc)]),
            sorted(self.db.get_from_index('test-idx', [('underneath',)])))

    def test_put_adds_to_index(self):
        self.db.create_index('test-idx', ['key'])
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
            self.db.get_from_index('test-idx', [('value',)]))

    def test_put_updates_index(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        new_doc_rev = self.db.put_doc(doc_id, doc_rev, new_doc)
        self.assertEqual([],
            self.db.get_from_index('test-idx', [('value',)]))
        self.assertEqual([(doc_id, new_doc_rev, new_doc)],
            self.db.get_from_index('test-idx', [('altval',)]))

    def test_get_all_from_index(self):
        self.db.create_index('test-idx', ['key'])
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        doc2_id, doc2_rev = self.db.create_doc(nested_doc)
        # This one should not be in the index
        doc3_id, doc3_rev = self.db.create_doc('{"no": "key"}')
        diff_value_doc = '{"key": "diff value"}'
        doc4_id, doc4_rev = self.db.create_doc(diff_value_doc)
        # This is essentially a 'prefix' match, but we match every entry.
        self.assertEqual(sorted([
            (doc1_id, doc1_rev, simple_doc),
            (doc2_id, doc2_rev, nested_doc),
            (doc4_id, doc4_rev, diff_value_doc)]),
            sorted(self.db.get_from_index('test-idx', [('*',)])))

    def test_get_from_index_case_sensitive(self):
        self.db.create_index('test-idx', ['key'])
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        self.assertEqual([], self.db.get_from_index('test-idx', [('V*',)]))
        self.assertEqual([(doc1_id, doc1_rev, simple_doc)],
                         self.db.get_from_index('test-idx', [('v*',)]))

    def test_get_from_index_empty_string(self):
        self.db.create_index('test-idx', ['key'])
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        doc2 = '{"key": ""}'
        doc2_id, doc2_rev = self.db.create_doc(doc2)
        self.assertEqual([(doc2_id, doc2_rev, doc2)],
                         self.db.get_from_index('test-idx', [('',)]))
        # Empty string matches the wildcard.
        self.assertEqual(sorted([
            (doc1_id, doc1_rev, simple_doc),
            (doc2_id, doc2_rev, doc2)]),
            sorted(self.db.get_from_index('test-idx', [('*',)])))

    def test_get_from_index_illegal_number_of_entries(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [()])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('v1',)])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('v1', 'v2', 'v3')])

    def test_get_from_index_illegal_wildcards(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('v*', 'v2')])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('*', 'v2')])

    def test_get_from_index_with_sql_wildcards(self):
        self.db.create_index('test-idx', ['key'])
        doc1 = '{"key": "va%lue"}'
        doc2 = '{"key": "value"}'
        doc3 = '{"key": "va_lue"}'
        doc1_id, doc1_rev = self.db.create_doc(doc1)
        doc2_id, doc2_rev = self.db.create_doc(doc2)
        doc3_id, doc3_rev = self.db.create_doc(doc3)
        # The '%' in the search should be treated literally, not as a sql
        # globbing character.
        self.assertEqual(sorted([(doc1_id, doc1_rev, doc1)]),
            sorted(self.db.get_from_index('test-idx', [('va%*',)])))
        # Same for '_'
        self.assertEqual(sorted([(doc3_id, doc3_rev, doc3)]),
            sorted(self.db.get_from_index('test-idx', [('va_*',)])))

    def test_get_from_index_not_null(self):
        self.db.create_index('test-idx', ['key'])
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        doc2_id, doc2_rev = self.db.create_doc('{"key": null}')
        self.assertEqual(sorted([
            (doc1_id, doc1_rev, simple_doc)]),
            self.db.get_from_index('test-idx', [('*',)]))

    def test_get_partial_from_index(self):
        doc1 = '{"k1": "v1", "k2": "v2"}'
        doc2 = '{"k1": "v1", "k2": "x2"}'
        doc3 = '{"k1": "v1", "k2": "y2"}'
        # doc4 has a different k1 value, so it doesn't match the prefix.
        doc4 = '{"k1": "NN", "k2": "v2"}'
        doc1_id, doc1_rev = self.db.create_doc(doc1)
        doc2_id, doc2_rev = self.db.create_doc(doc2)
        doc3_id, doc3_rev = self.db.create_doc(doc3)
        doc4_id, doc4_rev = self.db.create_doc(doc4)
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertEqual(sorted([
            (doc1_id, doc1_rev, doc1),
            (doc2_id, doc2_rev, doc2),
            (doc3_id, doc3_rev, doc3)]),
            sorted(self.db.get_from_index('test-idx', [("v1", "*")])))

    def test_get_glob_match(self):
        # Note: the exact glob syntax is probably subject to change
        doc1 = '{"k1": "v1", "k2": "v1"}'
        doc2 = '{"k1": "v1", "k2": "v2"}'
        doc3 = '{"k1": "v1", "k2": "v3"}'
        # doc4 has a different k2 prefix value, so it doesn't match
        doc4 = '{"k1": "v1", "k2": "ZZ"}'
        self.db.create_index('test-idx', ['k1', 'k2'])
        doc1_id, doc1_rev = self.db.create_doc(doc1)
        doc2_id, doc2_rev = self.db.create_doc(doc2)
        doc3_id, doc3_rev = self.db.create_doc(doc3)
        doc4_id, doc4_rev = self.db.create_doc(doc4)
        self.assertEqual(sorted([
            (doc1_id, doc1_rev, doc1),
            (doc2_id, doc2_rev, doc2),
            (doc3_id, doc3_rev, doc3)]),
            sorted(self.db.get_from_index('test-idx', [("v1", "v*")])))

    def test_delete_updates_index(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        doc2_id, doc2_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([(doc_id, doc_rev, simple_doc),
                          (doc2_id, doc2_rev, simple_doc)],
            self.db.get_from_index('test-idx', [('value',)]))
        self.db.delete_doc(doc_id, doc_rev)
        self.assertEqual([(doc2_id, doc2_rev, simple_doc)],
            self.db.get_from_index('test-idx', [('value',)]))

    def test_delete_index(self):
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([('test-idx', ['key'])], self.db.list_indexes())
        self.db.delete_index('test-idx')
        self.assertEqual([], self.db.list_indexes())

    def test__sync_exchange_updates_indexes(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        other_rev = 'test:1|z:2'
        st = self.db.get_sync_target()
        result = st.sync_exchange([(doc_id, other_rev, new_doc)],
                                  'other-machine', from_machine_rev=10,
                                  last_known_rev=0)
        self.assertEqual((other_rev, new_doc, False), self.db.get_doc(doc_id))
        self.assertEqual([(doc_id, other_rev, new_doc)],
                         self.db.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db.get_from_index('test-idx', [('value',)]))


class DatabaseSyncTargetTests(DatabaseBaseTests):

    def setUp(self):
        super(DatabaseSyncTargetTests, self).setUp()
        self.db = self.create_database('test')
        self.st = self.db.get_sync_target()

    def test_get_sync_target(self):
        self.assertIsNot(None, self.st)

    def test_get_sync_info(self):
        self.assertEqual(('test', 0, 0), self.st.get_sync_info('other'))

    def test_create_doc_updates_sync_info(self):
        self.assertEqual(('test', 0, 0), self.st.get_sync_info('other'))
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual(('test', 1, 0), self.st.get_sync_info('other'))

    def test_record_sync_info(self):
        self.assertEqual(('test', 0, 0), self.st.get_sync_info('machine'))
        self.st.record_sync_info('machine', 10)
        self.assertEqual(('test', 0, 10), self.st.get_sync_info('machine'))

    def test_sync_exchange(self):
        result = self.st.sync_exchange([('doc-id', 'machine:1', simple_doc)],
                                       'machine', from_machine_rev=10,
                                       last_known_rev=0)
        self.assertEqual(('machine:1', simple_doc, False),
                         self.db.get_doc('doc-id'))
        self.assertEqual(['doc-id'], self.db._get_transaction_log())
        self.assertEqual(([], [], 1), result)
        self.assertEqual(10, self.st.get_sync_info('machine')[-1])

    def test_sync_exchange_refuses_conflicts(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        new_doc = '{"key": "altval"}'
        result = self.st.sync_exchange([(doc_id, 'machine:1', new_doc)],
                                       'machine', from_machine_rev=10,
                                       last_known_rev=0)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        self.assertEqual(([], [(doc_id, doc_rev, simple_doc)], 1), result)

    def test_sync_exchange_ignores_convergence(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        result = self.st.sync_exchange([(doc_id, doc_rev, simple_doc)],
                                       'machine', from_machine_rev=10,
                                       last_known_rev=1)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        self.assertEqual(([], [], 1), result)

    def test_sync_exchange_returns_new_docs(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        result = self.st.sync_exchange([], 'other-machine',
                                       from_machine_rev=10, last_known_rev=0)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        self.assertEqual(([(doc_id, doc_rev, simple_doc)], [], 1), result)

    def test_sync_exchange_getting_newer_docs(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        new_doc = '{"key": "altval"}'
        result = self.st.sync_exchange([(doc_id, 'test:1|z:2', new_doc)],
                                        'other-machine', from_machine_rev=10,
                                        last_known_rev=0)
        self.assertEqual([doc_id, doc_id], self.db._get_transaction_log())
        self.assertEqual(([], [], 2), result)

    def test_sync_exchange_with_concurrent_updates(self):
        doc_id, doc_rev = self.db.create_doc(simple_doc)
        self.assertEqual([doc_id], self.db._get_transaction_log())
        orig_wc = self.db.whats_changed
        def after_whatschanged(*args, **kwargs):
            val = orig_wc(*args, **kwargs)
            self.db.create_doc('{"new": "doc"}')
            return val
        self.db.whats_changed = after_whatschanged
        new_doc = '{"key": "altval"}'
        result = self.st.sync_exchange([(doc_id, 'test:1|z:2', new_doc)],
                                       'other-machine', from_machine_rev=10,
                                       last_known_rev=0)
        self.assertEqual(([], [], 2), result)


class DatabaseSyncTests(DatabaseBaseTests):

    def setUp(self):
        super(DatabaseSyncTests, self).setUp()
        self.db1 = self.create_database('test1')
        self.db2 = self.create_database('test2')

    def test_sync_tracks_db_rev_of_other(self):
        self.assertEqual(0, self.db1.sync(self.db2))
        self.assertEqual(0, self.db1.get_sync_generation('test2'))
        self.assertEqual(0, self.db2.get_sync_generation('test1'))
        self.assertEqual({'receive': {'docs': [], 'from_id': 'test1',
                                      'from_rev': 0, 'last_known_rev': 0},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 0}},
                         self.db2._last_exchange_log)

    def test_sync_puts_changes(self):
        doc_id, doc_rev = self.db1.create_doc(simple_doc)
        self.assertEqual(1, self.db1.sync(self.db2))
        self.assertEqual((doc_rev, simple_doc, False), self.db2.get_doc(doc_id))
        self.assertEqual(1, self.db1.get_sync_generation('test2'))
        self.assertEqual(1, self.db2.get_sync_generation('test1'))
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 1}},
                         self.db2._last_exchange_log)

    def test_sync_pulls_changes(self):
        doc_id, doc_rev = self.db2.create_doc(simple_doc)
        self.db1.create_index('test-idx', ['key'])
        self.assertEqual(0, self.db1.sync(self.db2))
        self.assertEqual((doc_rev, simple_doc, False), self.db1.get_doc(doc_id))
        self.assertEqual(1, self.db1.get_sync_generation('test2'))
        self.assertEqual(1, self.db2.get_sync_generation('test1'))
        self.assertEqual({'receive': {'docs': [], 'from_id': 'test1',
                                      'from_rev': 0, 'last_known_rev': 0},
                          'return': {'new_docs': [(doc_id, doc_rev)],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.db2._last_exchange_log)
        self.assertEqual([(doc_id, doc_rev, simple_doc)],
                         self.db1.get_from_index('test-idx', [('value',)]))

    def test_sync_pulling_doesnt_update_other_if_changed(self):
        doc_id, doc_rev = self.db2.create_doc(simple_doc)
        # Right after we call c2._sync_exchange, we update our local database
        # with a new record. When we finish synchronizing, we can notice that
        # something locally was updated, and we cannot tell c2 our new updated
        # db_rev
        orig = self.db1.put_docs
        def after_put_docs(*args, **kwargs):
            result = orig(*args, **kwargs)
            self.db1.create_doc(simple_doc)
            return result
        self.db1.put_docs = after_put_docs
        self.assertEqual(0, self.db1.sync(self.db2))
        self.assertEqual({'receive': {'docs': [], 'from_id': 'test1',
                                      'from_rev': 0, 'last_known_rev': 0},
                          'return': {'new_docs': [(doc_id, doc_rev)],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.db2._last_exchange_log)
        self.assertEqual(1, self.db1.get_sync_generation('test2'))
        # c2 should not have gotten a '_record_sync_info' call, because the
        # local database had been updated more than just by the messages
        # returned from c2.
        self.assertEqual(0, self.db2.get_sync_generation('test1'))

    def test_sync_ignores_convergence(self):
        doc_id, doc_rev = self.db1.create_doc(simple_doc)
        self.db3 = self.create_database('test3')
        self.assertEqual(1, self.db1.sync(self.db3))
        self.assertEqual(0, self.db2.sync(self.db3))
        self.assertEqual(1, self.db1.sync(self.db2))
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [],
                                     'conf_docs': [], 'last_rev': 1}},
                         self.db2._last_exchange_log)

    def test_sync_ignores_superseded(self):
        doc_id, doc_rev = self.db1.create_doc(simple_doc)
        self.db3 = self.create_database('test3')
        self.db1.sync(self.db3)
        self.db2.sync(self.db3)
        new_doc = '{"key": "altval"}'
        doc_rev2 = self.db1.put_doc(doc_id, doc_rev, new_doc)
        self.db2.sync(self.db1)
        self.assertEqual({'receive': {'docs': [(doc_id, doc_rev)],
                                      'from_id': 'test2',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [(doc_id, doc_rev2)],
                                     'conf_docs': [], 'last_rev': 2}},
                         self.db1._last_exchange_log)
        self.assertEqual((doc_rev2, new_doc, False), self.db1.get_doc(doc_id))


    def test_sync_sees_remote_conflicted(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        self.db1.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc, doc_id=doc_id)
        self.assertEqual([doc_id], self.db1._get_transaction_log())
        self.db1.sync(self.db2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc1_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 1, 'last_known_rev': 0},
                          'return': {'new_docs': [],
                                     'conf_docs': [(doc_id, doc2_rev)],
                                     'last_rev': 1}},
                         self.db2._last_exchange_log)
        self.assertEqual([doc_id, doc_id], self.db1._get_transaction_log())
        self.assertEqual((doc2_rev, new_doc, True), self.db1.get_doc(doc_id))
        self.assertEqual((doc2_rev, new_doc, False), self.db2.get_doc(doc_id))
        self.assertEqual([(doc_id, doc2_rev, new_doc)],
                         self.db1.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))

    def test_sync_sees_remote_delete_conflicted(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        self.db1.create_index('test-idx', ['key'])
        self.db1.sync(self.db2)
        doc2_rev = doc1_rev
        new_doc = '{"key": "altval"}'
        doc1_rev = self.db1.put_doc(doc_id, doc1_rev, new_doc)
        doc2_rev = self.db2.delete_doc(doc_id, doc2_rev)
        self.assertEqual([doc_id, doc_id], self.db1._get_transaction_log())
        self.db1.sync(self.db2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc1_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 2, 'last_known_rev': 1},
                          'return': {'new_docs': [],
                                     'conf_docs': [(doc_id, doc2_rev)],
                                     'last_rev': 2}},
                         self.db2._last_exchange_log)
        self.assertEqual([doc_id, doc_id, doc_id],
                         self.db1._get_transaction_log())
        self.assertEqual((doc2_rev, None, True), self.db1.get_doc(doc_id))
        self.assertEqual((doc2_rev, None, False), self.db2.get_doc(doc_id))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))

    def test_sync_local_race_conflicted(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        self.db1.create_index('test-idx', ['key'])
        self.db1.sync(self.db2)
        new_doc1 = '{"key": "localval"}'
        new_doc2 = '{"key": "altval"}'
        doc2_rev2 = self.db2.put_doc(doc_id, doc1_rev, new_doc2)
        # Monkey patch so that after the local client has determined recent
        # changes, we get another one, before sync finishes.
        orig_wc = self.db1.whats_changed
        def after_whatschanged(*args, **kwargs):
            val = orig_wc(*args, **kwargs)
            self.db1.put_doc(doc_id, doc1_rev, new_doc1)
            return val
        self.db1.whats_changed = after_whatschanged
        self.db1.sync(self.db2)
        self.assertEqual((doc2_rev2, new_doc2, True), self.db1.get_doc(doc_id))
        self.assertEqual([(doc_id, doc2_rev2, new_doc2)],
                         self.db1.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('localval',)]))

    def test_sync_propagates_deletes(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        self.db1.create_index('test-idx', ['key'])
        self.db1.sync(self.db2)
        self.db2.create_index('test-idx', ['key'])
        self.db3 = self.create_database('test3')
        self.db1.sync(self.db3)
        deleted_rev = self.db1.delete_doc(doc_id, doc1_rev)
        self.db1.sync(self.db2)
        self.assertEqual({'receive': {'docs': [(doc_id, deleted_rev)],
                                      'from_id': 'test1',
                                      'from_rev': 2, 'last_known_rev': 1},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 2}},
                         self.db2._last_exchange_log)
        self.assertEqual((deleted_rev, None, False), self.db1.get_doc(doc_id))
        self.assertEqual((deleted_rev, None, False), self.db2.get_doc(doc_id))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))
        self.assertEqual([], self.db2.get_from_index('test-idx', [('value',)]))
        self.db2.sync(self.db3)
        self.assertEqual({'receive': {'docs': [(doc_id, deleted_rev)],
                                      'from_id': 'test2',
                                      'from_rev': 2, 'last_known_rev': 0},
                          'return': {'new_docs': [], 'conf_docs': [],
                                     'last_rev': 2}},
                         self.db3._last_exchange_log)
        self.assertEqual((deleted_rev, None, False), self.db3.get_doc(doc_id))

    def test_put_refuses_to_update_conflicted(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc1, doc_id=doc_id)
        self.db1.sync(self.db2)
        self.assertEqual((doc2_rev, new_doc1, True), self.db1.get_doc(doc_id))
        new_doc2 = '{"key": "local"}'
        self.assertRaises(errors.ConflictedDoc,
            self.db1.put_doc, doc_id, doc2_rev, new_doc2)

    def test_delete_refuses_for_conflicted(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc1, doc_id=doc_id)
        self.db1.sync(self.db2)
        self.assertEqual((doc2_rev, new_doc1, True), self.db1.get_doc(doc_id))
        self.assertRaises(errors.ConflictedDoc,
            self.db1.delete_doc, doc_id, doc2_rev)

    def test_get_doc_conflicts_unconflicted(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        self.assertEqual([], self.db1.get_doc_conflicts(doc_id))

    def test_get_doc_conflicts_no_such_id(self):
        self.assertEqual([], self.db1.get_doc_conflicts('doc-id'))

    def test_get_doc_conflicts(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc1, doc_id=doc_id)
        self.db1.sync(self.db2)
        self.assertEqual([(doc2_rev, new_doc1),
                          (doc1_rev, simple_doc)],
                         self.db1.get_doc_conflicts(doc_id))

    def test_resolve_doc(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc1, doc_id=doc_id)
        self.db1.sync(self.db2)
        self.assertEqual([(doc2_rev, new_doc1),
                          (doc1_rev, simple_doc)],
                         self.db1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.db1.resolve_doc(doc_id, simple_doc,
                                                     [doc2_rev, doc1_rev])
        self.assertFalse(has_conflicts)
        self.assertEqual((new_rev, simple_doc, False), self.db1.get_doc(doc_id))
        self.assertEqual([], self.db1.get_doc_conflicts(doc_id))

    def test_resolve_doc_picks_biggest_vcr(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        doc1_rev = self.db1.put_doc(doc_id, doc1_rev, simple_doc)
        new_doc1 = '{"key": "altval"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc1, doc_id=doc_id)
        doc2_rev = self.db2.put_doc(doc_id, doc2_rev, new_doc1)
        self.db1.sync(self.db2)
        self.assertEqual([(doc2_rev, new_doc1),
                          (doc1_rev, simple_doc)],
                         self.db1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.db1.resolve_doc(doc_id, simple_doc,
                                                     [doc2_rev, doc1_rev])
        self.assertFalse(has_conflicts)
        self.assertEqual((new_rev, simple_doc, False), self.db1.get_doc(doc_id))
        self.assertEqual([], self.db1.get_doc_conflicts(doc_id))
        vcr_1 = vectorclock.VectorClockRev(doc1_rev)
        vcr_2 = vectorclock.VectorClockRev(doc2_rev)
        vcr_new = vectorclock.VectorClockRev(new_rev)
        self.assertTrue(vcr_new.is_newer(vcr_1))
        self.assertTrue(vcr_new.is_newer(vcr_2))

    def test_resolve_doc_partial_not_winning(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        new_doc2 = '{"key": "valin2"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc2, doc_id=doc_id)
        self.db1.sync(self.db2)
        self.assertEqual([(doc2_rev, new_doc2),
                          (doc1_rev, simple_doc)],
                         self.db1.get_doc_conflicts(doc_id))
        self.db3 = self.create_database('test3')
        new_doc3 = '{"key": "valin3"}'
        doc_id, doc3_rev = self.db3.create_doc(new_doc3, doc_id=doc_id)
        self.db1.sync(self.db3)
        self.assertEqual([(doc3_rev, new_doc3),
                          (doc1_rev, simple_doc),
                          (doc2_rev, new_doc2)],
                         self.db1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.db1.resolve_doc(doc_id, simple_doc,
                                                     [doc2_rev, doc1_rev])
        self.assertTrue(has_conflicts)
        self.assertEqual((doc3_rev, new_doc3, True), self.db1.get_doc(doc_id))
        self.assertEqual([(doc3_rev, new_doc3), (new_rev, simple_doc)],
                         self.db1.get_doc_conflicts(doc_id))

    def test_resolve_doc_partial_winning(self):
        doc_id, doc1_rev = self.db1.create_doc(simple_doc)
        new_doc2 = '{"key": "valin2"}'
        doc_id, doc2_rev = self.db2.create_doc(new_doc2, doc_id=doc_id)
        self.db1.sync(self.db2)
        self.db3 = self.create_database('test3')
        new_doc3 = '{"key": "valin3"}'
        doc_id, doc3_rev = self.db3.create_doc(new_doc3, doc_id=doc_id)
        self.db1.sync(self.db3)
        self.assertEqual([(doc3_rev, new_doc3),
                          (doc1_rev, simple_doc),
                          (doc2_rev, new_doc2)],
                         self.db1.get_doc_conflicts(doc_id))
        new_rev, has_conflicts = self.db1.resolve_doc(doc_id, simple_doc,
                                                     [doc3_rev, doc1_rev])
        self.assertTrue(has_conflicts)
        self.assertEqual([(new_rev, simple_doc),
                          (doc2_rev, new_doc2)],
                         self.db1.get_doc_conflicts(doc_id))


# Use a custom loader to apply the scenarios at load time.
load_tests = tests.load_with_scenarios
