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
    )


simple_doc = tests.simple_doc
nested_doc = tests.nested_doc


class DatabaseTests(tests.DatabaseBaseTests):

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
        self.assertEqual([(doc1_id, doc1_rev, simple_doc, False),
                          (doc2_id, doc2_rev, nested_doc, False)],
                         self.db.get_docs([doc1_id, doc2_id]))

    def test_get_docs_conflicted(self):
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        self.db.force_doc_sync_conflict(doc1_id, 'alternate:1', nested_doc)
        self.assertEqual([(doc1_id, 'alternate:1', nested_doc, True)],
                         self.db.get_docs([doc1_id]))

    def test_get_docs_conflicts_ignored(self):
        doc1_id, doc1_rev = self.db.create_doc(simple_doc)
        doc2_id, doc2_rev = self.db.create_doc(nested_doc)
        self.db.force_doc_sync_conflict(doc1_id, 'alternate:1', nested_doc)
        self.assertEqual(
            sorted([(doc1_id, 'alternate:1', nested_doc, None),
                    (doc2_id, doc2_rev, nested_doc, None)]),
            sorted(self.db.get_docs([doc1_id, doc2_id],
                                    check_for_conflicts=False)))

    def test_put_doc_creating_initial(self):
        new_rev = self.db.put_doc('my_doc_id', None, simple_doc)
        self.assertEqual((new_rev, simple_doc, False),
                         self.db.get_doc('my_doc_id'))

    def test_simple_put_doc_if_newer(self):
        state = self.db.put_doc_if_newer('my-doc-id', 'test:1', simple_doc)
        self.assertEqual('inserted', state)
        self.assertEqual(('test:1', simple_doc, False),
                         self.db.get_doc('my-doc-id'))

    def test_put_doc_if_newer_already_superseded(self):
        orig_doc = '{"new": "doc"}'
        doc1_id, doc1_rev1 = self.db.create_doc(orig_doc)
        doc1_rev2 = self.db.put_doc(doc1_id, doc1_rev1, simple_doc)
        # Nothing is inserted, because the document is already superseded
        state = self.db.put_doc_if_newer(doc1_id, doc1_rev1, orig_doc)
        self.assertEqual('superseded', state)
        self.assertEqual((doc1_rev2, simple_doc, False),
                         self.db.get_doc(doc1_id))

    def test_put_doc_if_newer_already_converged(self):
        orig_doc = '{"new": "doc"}'
        doc1_id, doc1_rev1 = self.db.create_doc(orig_doc)
        state = self.db.put_doc_if_newer(doc1_id, doc1_rev1, orig_doc)
        self.assertEqual('converged', state)

    def test_put_doc_if_newer_conflicted(self):
        doc1_id, doc1_rev1 = self.db.create_doc(simple_doc)
        # Nothing is inserted, the document id is returned as would-conflict
        state = self.db.put_doc_if_newer(doc1_id, 'alternate:1', nested_doc)
        self.assertEqual('conflicted', state)
        # The database wasn't altered
        self.assertEqual((doc1_rev1, simple_doc, False),
                         self.db.get_doc(doc1_id))

    def test_force_doc_with_conflict(self):
        doc1_id, doc1_rev1 = self.db.create_doc(simple_doc)
        self.db.force_doc_sync_conflict(doc1_id, 'alternate:1', nested_doc)
        self.assertEqual(('alternate:1', nested_doc, True),
                         self.db.get_doc(doc1_id))
        self.assertEqual([('alternate:1', nested_doc),
                          (doc1_rev1, simple_doc)],
                         self.db.get_doc_conflicts(doc1_id))

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
        db_gen, _ = self.db.whats_changed()
        self.db.delete_doc(doc_id, doc_rev)
        self.assertEqual((2, set([doc_id])), self.db.whats_changed(db_gen))

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


class DatabaseIndexTests(tests.DatabaseBaseTests):

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
        def ignore(doc_id, doc_rev, doc):
            pass
        result = st.sync_exchange([(doc_id, other_rev, new_doc)],
                                  'other-replica',
                                  from_replica_generation=10,
                                  last_known_generation=0,
                                  take_other_doc = ignore)
        self.assertEqual((other_rev, new_doc, False), self.db.get_doc(doc_id))
        self.assertEqual([(doc_id, other_rev, new_doc)],
                         self.db.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db.get_from_index('test-idx', [('value',)]))

# Use a custom loader to apply the scenarios at load time.
load_tests = tests.load_with_scenarios
