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
    Document,
    errors,
    tests,
    )


simple_doc = tests.simple_doc
nested_doc = tests.nested_doc


from u1db.tests.test_remote_sync_target import (
    http_server_def,
)

from u1db.remote import (
    http_database
    )


def http_create_database(test, replica_uid):
    test.startServer()
    db = test.request_state._create_database(replica_uid)
    return http_database.HTTPDatabase(test.getURL('test'))


class AllDatabaseTests(tests.DatabaseBaseTests, tests.TestCaseWithServer):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + [
        ('http', {'do_create_database': http_create_database,
                  'server_def': http_server_def}),
        ]

    def test_create_doc_allocating_doc_id(self):
        doc = self.db.create_doc(simple_doc)
        self.assertNotEqual(None, doc.doc_id)
        self.assertNotEqual(None, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_create_doc_with_id(self):
        doc = self.db.create_doc(simple_doc, doc_id='my-id')
        self.assertEqual('my-id', doc.doc_id)
        self.assertNotEqual(None, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_create_doc_existing_id(self):
        doc = self.db.create_doc(simple_doc)
        new_content = '{"something": "else"}'
        self.assertRaises(errors.RevisionConflict, self.db.create_doc,
                          new_content, doc.doc_id)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_put_doc_creating_initial(self):
        doc = Document('my_doc_id', None, simple_doc)
        new_rev = self.db.put_doc(doc)
        self.assertGetDoc(self.db, 'my_doc_id', new_rev, simple_doc, False)

    def test_put_doc_refuses_no_id(self):
        doc = Document(None, None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_put_doc_refuses_non_existing_old_rev(self):
        doc = Document('doc-id', 'test:4', simple_doc)
        self.assertRaises(errors.RevisionConflict, self.db.put_doc, doc)


class LocalDatabaseTests(tests.DatabaseBaseTests):

    def test_close(self):
        self.db.close()

    def test_get_docs(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual(sorted([doc1, doc2]),
                         sorted(self.db.get_docs([doc1.doc_id, doc2.doc_id])))

    def test_get_docs_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = Document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.force_doc_sync_conflict(doc2)
        self.assertEqual([doc2], self.db.get_docs([doc1.doc_id]))

    def test_get_docs_conflicts_ignored(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        alt_doc = Document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.force_doc_sync_conflict(alt_doc)
        self.assertEqual(
            sorted([Document(doc1.doc_id, 'alternate:1', nested_doc),
                    Document(doc2.doc_id, doc2.rev, nested_doc)]),
            sorted(self.db.get_docs([doc1.doc_id, doc2.doc_id],
                                    check_for_conflicts=False)))

    def test_get_docs_empty_list(self):
        self.assertEqual([], self.db.get_docs([]))

    def test_simple_put_doc_if_newer(self):
        doc = Document('my-doc-id', 'test:1', simple_doc)
        state = self.db.put_doc_if_newer(doc)
        self.assertEqual('inserted', state)
        self.assertGetDoc(self.db, 'my-doc-id', 'test:1', simple_doc, False)

    def test_put_doc_if_newer_already_superseded(self):
        orig_doc = '{"new": "doc"}'
        doc1 = self.db.create_doc(orig_doc)
        doc1_rev1 = doc1.rev
        doc1.content = simple_doc
        doc1_rev2 = self.db.put_doc(doc1)
        # Nothing is inserted, because the document is already superseded
        doc = Document(doc1.doc_id, doc1_rev1, orig_doc)
        state = self.db.put_doc_if_newer(doc)
        self.assertEqual('superseded', state)
        self.assertGetDoc(self.db, doc1.doc_id, doc1_rev2, simple_doc, False)

    def test_put_doc_if_newer_already_converged(self):
        orig_doc = '{"new": "doc"}'
        doc1 = self.db.create_doc(orig_doc)
        state = self.db.put_doc_if_newer(doc1)
        self.assertEqual('converged', state)

    def test_put_doc_if_newer_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        # Nothing is inserted, the document id is returned as would-conflict
        alt_doc = Document(doc1.doc_id, 'alternate:1', nested_doc)
        state = self.db.put_doc_if_newer(alt_doc)
        self.assertEqual('conflicted', state)
        # The database wasn't altered
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)

    def test_force_doc_with_conflict(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = Document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.force_doc_sync_conflict(doc2)
        self.assertTrue(doc2.has_conflicts)
        self.assertGetDoc(self.db, doc1.doc_id, 'alternate:1', nested_doc, True)
        self.assertEqual([('alternate:1', nested_doc),
                          (doc1.rev, simple_doc)],
                         self.db.get_doc_conflicts(doc1.doc_id))

    def test_get_doc_after_put(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.assertGetDoc(self.db, 'my_doc_id', doc.rev, simple_doc, False)

    def test_get_doc_nonexisting(self):
        self.assertIs(None, self.db.get_doc('non-existing'))

    def test_get_sync_generation(self):
        self.assertEqual(0, self.db.get_sync_generation('other-db'))
        self.db.set_sync_generation('other-db', 2)
        self.assertEqual(2, self.db.get_sync_generation('other-db'))

    def test_put_fails_with_bad_old_rev(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        old_rev = doc.rev
        doc.rev = 'other:1'
        doc.content = '{"something": "else"}'
        self.assertRaises(errors.RevisionConflict, self.db.put_doc, doc)
        self.assertGetDoc(self.db, 'my_doc_id', old_rev, simple_doc, False)

    def test_delete_doc(self):
        doc = self.db.create_doc(simple_doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)
        orig_rev = doc.rev
        self.db.delete_doc(doc)
        self.assertNotEqual(orig_rev, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, None, False)
        self.assertIsNot(None, self.db.get_doc(doc.doc_id))

    def test_delete_doc_non_existant(self):
        doc = Document('non-existing', 'other:1', simple_doc)
        self.assertRaises(KeyError,
            self.db.delete_doc, doc)

    def test_delete_doc_already_deleted(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertRaises(KeyError, self.db.delete_doc, doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, None, False)

    def test_delete_doc_bad_rev(self):
        doc1 = self.db.create_doc(simple_doc)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)
        doc2 = Document(doc1.doc_id, 'other:1', simple_doc)
        self.assertRaises(errors.RevisionConflict, self.db.delete_doc, doc2)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)

    def test_put_updates_transaction_log(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        doc.content = '{"something": "else"}'
        self.db.put_doc(doc)
        self.assertEqual([doc.doc_id, doc.doc_id],
                         self.db._get_transaction_log())
        self.assertEqual((2, set([doc.doc_id])), self.db.whats_changed())

    def test_delete_updates_transaction_log(self):
        doc = self.db.create_doc(simple_doc)
        db_gen, _ = self.db.whats_changed()
        self.db.delete_doc(doc)
        self.assertEqual((2, set([doc.doc_id])), self.db.whats_changed(db_gen))

    def test_whats_changed_initial_database(self):
        self.assertEqual((0, set()), self.db.whats_changed())

    def test_whats_changed_returns_one_id_for_multiple_changes(self):
        doc = self.db.create_doc(simple_doc)
        doc.content = '{"new": "contents"}'
        self.db.put_doc(doc)
        self.assertEqual((2, set([doc.doc_id])), self.db.whats_changed())
        self.assertEqual((2, set()), self.db.whats_changed(2))

    def test_handles_nested_content(self):
        doc = self.db.create_doc(nested_doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, nested_doc, False)

    def test_handles_doc_with_null(self):
        doc = self.db.create_doc('{"key": null}')
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, '{"key": null}', False)


class DatabaseIndexTests(tests.DatabaseBaseTests):

    def test_create_index(self):
        self.db.create_index('test-idx', ['name'])
        self.assertEqual([('test-idx', ['name'])],
                         self.db.list_indexes())

    def test_create_index_evaluates_it(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([doc],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_create_index_multiple_exact_matches(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual(sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', [('value',)])))

    def test_get_from_index(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([doc],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_unmatched(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([], self.db.get_from_index('test-idx', [('novalue',)]))

    def test_get_from_index_some_matches(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('value',), ('novalue',)]))

    def test_get_from_index_multi(self):
        content = '{"key": "value", "key2": "value2"}'
        doc = self.db.create_doc(content)
        self.db.create_index('test-idx', ['key', 'key2'])
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('value', 'value2')]))

    def test_nested_index(self):
        doc = self.db.create_doc(nested_doc)
        self.db.create_index('test-idx', ['sub.doc'])
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('underneath',)]))
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual(
            sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', [('underneath',)])))

    def test_create_adds_to_index(self):
        self.db.create_index('test-idx', ['key'])
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('value',)]))

    def test_put_updates_index(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        new_content = '{"key": "altval"}'
        doc.content = new_content
        new_doc_rev = self.db.put_doc(doc)
        self.assertEqual([],
            self.db.get_from_index('test-idx', [('value',)]))
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('altval',)]))

    def test_put_updates_when_adding_key(self):
        doc = self.db.create_doc("{}")
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([],
            self.db.get_from_index('test-idx', [('*',)]))
        doc.content = simple_doc
        self.db.put_doc(doc)
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('*',)]))

    def test_get_all_from_index(self):
        self.db.create_index('test-idx', ['key'])
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        # This one should not be in the index
        doc3 = self.db.create_doc('{"no": "key"}')
        diff_value_doc = '{"key": "diff value"}'
        doc4 = self.db.create_doc(diff_value_doc)
        # This is essentially a 'prefix' match, but we match every entry.
        self.assertEqual(sorted([doc1, doc2, doc4]),
            sorted(self.db.get_from_index('test-idx', [('*',)])))

    def test_get_from_index_case_sensitive(self):
        self.db.create_index('test-idx', ['key'])
        doc1 = self.db.create_doc(simple_doc)
        self.assertEqual([], self.db.get_from_index('test-idx', [('V*',)]))
        self.assertEqual([doc1],
                         self.db.get_from_index('test-idx', [('v*',)]))

    def test_get_from_index_empty_string(self):
        self.db.create_index('test-idx', ['key'])
        doc1 = self.db.create_doc(simple_doc)
        content2 = '{"key": ""}'
        doc2 = self.db.create_doc(content2)
        self.assertEqual([doc2],
                         self.db.get_from_index('test-idx', [('',)]))
        # Empty string matches the wildcard.
        self.assertEqual(sorted([doc1, doc2]),
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
        content1 = '{"key": "va%lue"}'
        content2 = '{"key": "value"}'
        content3 = '{"key": "va_lue"}'
        doc1 = self.db.create_doc(content1)
        doc2 = self.db.create_doc(content2)
        doc3 = self.db.create_doc(content3)
        # The '%' in the search should be treated literally, not as a sql
        # globbing character.
        self.assertEqual([doc1],
            self.db.get_from_index('test-idx', [('va%*',)]))
        # Same for '_'
        self.assertEqual([doc3],
            self.db.get_from_index('test-idx', [('va_*',)]))

    def test_get_from_index_not_null(self):
        self.db.create_index('test-idx', ['key'])
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc('{"key": null}')
        self.assertEqual([doc1],
            self.db.get_from_index('test-idx', [('*',)]))

    def test_get_partial_from_index(self):
        content1 = '{"k1": "v1", "k2": "v2"}'
        content2 = '{"k1": "v1", "k2": "x2"}'
        content3 = '{"k1": "v1", "k2": "y2"}'
        # doc4 has a different k1 value, so it doesn't match the prefix.
        content4 = '{"k1": "NN", "k2": "v2"}'
        doc1 = self.db.create_doc(content1)
        doc2 = self.db.create_doc(content2)
        doc3 = self.db.create_doc(content3)
        doc4 = self.db.create_doc(content4)
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertEqual(sorted([doc1, doc2, doc3]),
            sorted(self.db.get_from_index('test-idx', [("v1", "*")])))

    def test_get_glob_match(self):
        # Note: the exact glob syntax is probably subject to change
        content1 = '{"k1": "v1", "k2": "v1"}'
        content2 = '{"k1": "v1", "k2": "v2"}'
        content3 = '{"k1": "v1", "k2": "v3"}'
        # doc4 has a different k2 prefix value, so it doesn't match
        content4 = '{"k1": "v1", "k2": "ZZ"}'
        self.db.create_index('test-idx', ['k1', 'k2'])
        doc1 = self.db.create_doc(content1)
        doc2 = self.db.create_doc(content2)
        doc3 = self.db.create_doc(content3)
        doc4 = self.db.create_doc(content4)
        self.assertEqual(sorted([doc1, doc2, doc3]),
            sorted(self.db.get_from_index('test-idx', [("v1", "v*")])))

    def test_delete_updates_index(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual(sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', [('value',)])))
        self.db.delete_doc(doc)
        self.assertEqual([doc2],
            self.db.get_from_index('test-idx', [('value',)]))

    def test_delete_index(self):
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([('test-idx', ['key'])], self.db.list_indexes())
        self.db.delete_index('test-idx')
        self.assertEqual([], self.db.list_indexes())

    def test_sync_exchange_updates_indexes(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        new_content = '{"key": "altval"}'
        other_rev = 'test:1|z:2'
        st = self.db.get_sync_target()
        def ignore(doc_id, doc_rev, doc):
            pass
        docs = [Document(doc.doc_id, other_rev, new_content)]
        result = st.sync_exchange(docs, 'other-replica',
                                  from_replica_generation=10,
                                  last_known_generation=0,
                                  return_doc_cb=ignore)
        self.assertGetDoc(self.db, doc.doc_id, other_rev, new_content, False)
        self.assertEqual([Document(doc.doc_id, other_rev, new_content)],
                         self.db.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db.get_from_index('test-idx', [('value',)]))

# Use a custom loader to apply the scenarios at load time.
load_tests = tests.load_with_scenarios
