# Copyright 2011 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

"""The backend class for U1DB. This deals with hiding storage details."""

from u1db import (
    errors,
    tests,
    vectorclock,
    )

simple_doc = tests.simple_doc
nested_doc = tests.nested_doc

from u1db.tests.test_remote_sync_target import (
    http_server_def,
    oauth_http_server_def,
)

from u1db.remote import (
    http_database,
    )

try:
    from u1db.tests import c_backend_wrapper
except ImportError:
    c_backend_wrapper = None


def http_create_database(test, replica_uid, path='test'):
    test.startServer()
    test.request_state._create_database(replica_uid)
    return http_database.HTTPDatabase(test.getURL(path))


def oauth_http_create_database(test, replica_uid):
    http_db = http_create_database(test, replica_uid, '~/test')
    http_db.set_oauth_credentials(tests.consumer1.key, tests.consumer1.secret,
                                  tests.token1.key, tests.token1.secret)
    return http_db


class AllDatabaseTests(tests.DatabaseBaseTests, tests.TestCaseWithServer):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + [
        ('http', {'do_create_database': http_create_database,
                  'make_document': tests.create_doc,
                  'server_def': http_server_def}),
        ('oauth_http', {'do_create_database': oauth_http_create_database,
                        'make_document': tests.create_doc,
                        'server_def': oauth_http_server_def})
        ] + tests.C_DATABASE_SCENARIOS

    def test_close(self):
        self.db.close()

    def test_create_doc_allocating_doc_id(self):
        doc = self.db.create_doc(simple_doc)
        self.assertNotEqual(None, doc.doc_id)
        self.assertNotEqual(None, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)

    def test_create_doc_different_ids_same_db(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertNotEqual(doc1.doc_id, doc2.doc_id)

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
        doc = self.make_document('my_doc_id', None, simple_doc)
        new_rev = self.db.put_doc(doc)
        self.assertIsNot(None, new_rev)
        self.assertGetDoc(self.db, 'my_doc_id', new_rev, simple_doc, False)

    def test_put_doc_space_in_id(self):
        doc = self.make_document('my doc id', None, simple_doc)
        new_rev = self.db.put_doc(doc)
        self.assertIsNot(None, new_rev)
        self.assertGetDoc(self.db, 'my doc id', new_rev, simple_doc, False)

    def test_put_doc_update(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        orig_rev = doc.rev
        doc.content = '{"updated": "stuff"}'
        new_rev = self.db.put_doc(doc)
        self.assertNotEqual(new_rev, orig_rev)
        self.assertGetDoc(self.db, 'my_doc_id', new_rev,
                          '{"updated": "stuff"}', False)
        self.assertEqual(doc.rev, new_rev)

    def test_put_doc_refuses_no_id(self):
        doc = self.make_document(None, None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)
        doc = self.make_document("", None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_put_doc_refuses_slashes(self):
        doc = self.make_document('a/b', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)
        doc = self.make_document(r'\b', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_put_doc_refuses_non_existing_old_rev(self):
        doc = self.make_document('doc-id', 'test:4', simple_doc)
        self.assertRaises(errors.RevisionConflict, self.db.put_doc, doc)

    def test_put_fails_with_bad_old_rev(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        old_rev = doc.rev
        bad_doc = self.make_document(doc.doc_id, 'other:1',
                                     '{"something": "else"}')
        self.assertRaises(errors.RevisionConflict, self.db.put_doc, bad_doc)
        self.assertGetDoc(self.db, 'my_doc_id', old_rev, simple_doc, False)

    def test_get_doc_after_put(self):
        doc = self.db.create_doc(simple_doc, doc_id='my_doc_id')
        self.assertGetDoc(self.db, 'my_doc_id', doc.rev, simple_doc, False)

    def test_get_doc_nonexisting(self):
        self.assertIs(None, self.db.get_doc('non-existing'))

    def test_handles_nested_content(self):
        doc = self.db.create_doc(nested_doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, nested_doc, False)

    def test_handles_doc_with_null(self):
        doc = self.db.create_doc('{"key": null}')
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, '{"key": null}', False)

    def test_delete_doc(self):
        doc = self.db.create_doc(simple_doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)
        orig_rev = doc.rev
        self.db.delete_doc(doc)
        self.assertNotEqual(orig_rev, doc.rev)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, None, False)
        self.assertIsNot(None, self.db.get_doc(doc.doc_id))

    def test_delete_doc_non_existant(self):
        doc = self.make_document('non-existing', 'other:1', simple_doc)
        self.assertRaises(errors.DocumentDoesNotExist,
            self.db.delete_doc, doc)

    def test_delete_doc_already_deleted(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertRaises(errors.DocumentAlreadyDeleted,
                          self.db.delete_doc, doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, None, False)

    def test_delete_doc_bad_rev(self):
        doc1 = self.db.create_doc(simple_doc)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)
        doc2 = self.make_document(doc1.doc_id, 'other:1', simple_doc)
        self.assertRaises(errors.RevisionConflict, self.db.delete_doc, doc2)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)

    def test_delete_doc_sets_content_to_None(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertIs(None, doc.content)

    def test_delete_doc_rev_supersedes(self):
        doc = self.db.create_doc(simple_doc)
        doc.content = nested_doc
        self.db.put_doc(doc)
        doc.content = '{"fishy": "content"}'
        self.db.put_doc(doc)
        old_rev = doc.rev
        self.db.delete_doc(doc)
        cur_vc = vectorclock.VectorClockRev(old_rev)
        deleted_vc = vectorclock.VectorClockRev(doc.rev)
        self.assertTrue(deleted_vc.is_newer(cur_vc),
                "%s does not supersede %s" % (doc.rev, old_rev))

    def test_delete_then_put(self):
        doc = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, None, False)
        doc.content = nested_doc
        self.db.put_doc(doc)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, nested_doc, False)


class LocalDatabaseTests(tests.DatabaseBaseTests):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + tests.C_DATABASE_SCENARIOS

    def test_create_doc_different_ids_diff_db(self):
        doc1 = self.db.create_doc(simple_doc)
        db2 = self.create_database('other-uid')
        doc2 = db2.create_doc(simple_doc)
        self.assertNotEqual(doc1.doc_id, doc2.doc_id)

    def test_put_doc_refuses_slashes_picky(self):
        doc = self.make_document('/a', None, simple_doc)
        self.assertRaises(errors.InvalidDocId, self.db.put_doc, doc)

    def test_get_docs(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual([doc1, doc2],
                         self.db.get_docs([doc1.doc_id, doc2.doc_id]))

    def test_get_docs_request_ordered(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual([doc1, doc2],
                         self.db.get_docs([doc1.doc_id, doc2.doc_id]))
        self.assertEqual([doc2, doc1],
                         self.db.get_docs([doc2.doc_id, doc1.doc_id]))

    def test_get_docs_empty_list(self):
        self.assertEqual([], self.db.get_docs([]))

    def test_simple_put_doc_if_newer(self):
        doc = self.make_document('my-doc-id', 'test:1', simple_doc)
        state_at_gen = self.db.put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual(('inserted', 1), state_at_gen)
        self.assertGetDoc(self.db, 'my-doc-id', 'test:1', simple_doc, False)

    def test_simple_put_doc_if_newer_deleted(self):
        self.db.create_doc('{}', doc_id='my-doc-id')
        doc = self.make_document('my-doc-id', 'test:2', None)
        state_at_gen = self.db.put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual(('inserted', 2), state_at_gen)
        self.assertGetDoc(self.db, 'my-doc-id', 'test:2', None, False)

    def test_put_doc_if_newer_already_superseded(self):
        orig_doc = '{"new": "doc"}'
        doc1 = self.db.create_doc(orig_doc)
        doc1_rev1 = doc1.rev
        doc1.content = simple_doc
        self.db.put_doc(doc1)
        doc1_rev2 = doc1.rev
        # Nothing is inserted, because the document is already superseded
        doc = self.make_document(doc1.doc_id, doc1_rev1, orig_doc)
        state, _ = self.db.put_doc_if_newer(doc, save_conflict=False)
        self.assertEqual('superseded', state)
        self.assertGetDoc(self.db, doc1.doc_id, doc1_rev2, simple_doc, False)

    def test_put_doc_if_newer_already_converged(self):
        orig_doc = '{"new": "doc"}'
        doc1 = self.db.create_doc(orig_doc)
        state_at_gen = self.db.put_doc_if_newer(doc1, save_conflict=False)
        self.assertEqual(('converged', 1), state_at_gen)

    def test_put_doc_if_newer_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        # Nothing is inserted, the document id is returned as would-conflict
        alt_doc = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        state, _ = self.db.put_doc_if_newer(alt_doc, save_conflict=False)
        self.assertEqual('conflicted', state)
        # The database wasn't altered
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)

    def test_put_doc_if_newer_replica_uid(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.set_sync_generation('other', 1)
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|other:1',
                                  nested_doc)
        self.assertEqual('inserted',
            self.db.put_doc_if_newer(doc2, save_conflict=False,
                                     replica_uid='other', replica_gen=2)[0])
        self.assertEqual(2, self.db.get_sync_generation('other'))
        # Compare to the old rev, should be superseded
        doc2 = self.make_document(doc1.doc_id, doc1.rev, nested_doc)
        self.assertEqual('superseded',
            self.db.put_doc_if_newer(doc2, save_conflict=False,
                                     replica_uid='other', replica_gen=3)[0])
        self.assertEqual(3, self.db.get_sync_generation('other'))
        # A conflict that isn't saved still records the sync gen, because we
        # don't need to see it again
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|fourth:1',
                                  nested_doc)
        self.assertEqual('conflicted',
            self.db.put_doc_if_newer(doc2, save_conflict=False,
                                     replica_uid='other', replica_gen=4)[0])
        self.assertEqual(4, self.db.get_sync_generation('other'))

    def test_get_sync_generation(self):
        self.assertEqual(0, self.db.get_sync_generation('other-db'))
        self.db.set_sync_generation('other-db', 2)
        self.assertEqual(2, self.db.get_sync_generation('other-db'))

    def test_put_updates_transaction_log(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        doc.content = '{"something": "else"}'
        self.db.put_doc(doc)
        self.assertEqual([doc.doc_id, doc.doc_id],
                         self.db._get_transaction_log())
        self.assertEqual((2, [(doc.doc_id, 2)]), self.db.whats_changed())

    def test_delete_updates_transaction_log(self):
        doc = self.db.create_doc(simple_doc)
        db_gen, _ = self.db.whats_changed()
        self.db.delete_doc(doc)
        self.assertEqual((2, [(doc.doc_id, 2)]), self.db.whats_changed(db_gen))

    def test_whats_changed_initial_database(self):
        self.assertEqual((0, []), self.db.whats_changed())

    def test_whats_changed_returns_one_id_for_multiple_changes(self):
        doc = self.db.create_doc(simple_doc)
        doc.content = '{"new": "contents"}'
        self.db.put_doc(doc)
        self.assertEqual((2, [(doc.doc_id, 2)]), self.db.whats_changed())
        self.assertEqual((2, []), self.db.whats_changed(2))

    def test_whats_changed_returns_last_edits_ascending(self):
        doc = self.db.create_doc(simple_doc)
        doc1 = self.db.create_doc(simple_doc)
        doc.content = '{"new": "contents"}'
        self.db.delete_doc(doc1)
        self.db.put_doc(doc)
        self.assertEqual((4, [(doc1.doc_id, 3), (doc.doc_id, 4)]),
                         self.db.whats_changed())

    def test_whats_changed_doesnt_includ_old_gen(self):
        self.db.create_doc(simple_doc)
        self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.assertEqual((3, [(doc2.doc_id, 3)]),
                         self.db.whats_changed(2))


class LocalDatabaseWithConflictsTests(tests.DatabaseBaseTests):
    # test supporting/functionality around storing conflicts

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + tests.C_DATABASE_SCENARIOS

    def test_get_docs_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertEqual([doc2], self.db.get_docs([doc1.doc_id]))

    def test_get_docs_conflicts_ignored(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        alt_doc = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(alt_doc, save_conflict=True)
        no_conflict_doc = self.make_document(doc1.doc_id, 'alternate:1',
                                             nested_doc)
        self.assertEqual([no_conflict_doc, doc2],
                         self.db.get_docs([doc1.doc_id, doc2.doc_id],
                                          check_for_conflicts=False))

    def test_get_doc_conflicts(self):
        doc = self.db.create_doc(simple_doc)
        alt_doc = self.make_document(doc.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(alt_doc, save_conflict=True)
        self.assertEqual([alt_doc, doc],
                         self.db.get_doc_conflicts(doc.doc_id))

    def test_get_doc_conflicts_unconflicted(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([], self.db.get_doc_conflicts(doc.doc_id))

    def test_get_doc_conflicts_no_such_id(self):
        self.assertEqual([], self.db.get_doc_conflicts('doc-id'))

    def test_resolve_doc(self):
        doc = self.db.create_doc(simple_doc)
        alt_doc = self.make_document(doc.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(alt_doc, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc.doc_id,
            [('alternate:1', nested_doc), (doc.rev, simple_doc)])
        orig_rev = doc.rev
        self.db.resolve_doc(doc, [alt_doc.rev, doc.rev])
        self.assertNotEqual(orig_rev, doc.rev)
        self.assertFalse(doc.has_conflicts)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, simple_doc, False)
        self.assertGetDocConflicts(self.db, doc.doc_id, [])

    def test_resolve_doc_picks_biggest_vcr(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, simple_doc)])
        orig_doc1_rev = doc1.rev
        self.db.resolve_doc(doc1, [doc2.rev, doc1.rev])
        self.assertFalse(doc1.has_conflicts)
        self.assertNotEqual(orig_doc1_rev, doc1.rev)
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, simple_doc, False)
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        vcr_1 = vectorclock.VectorClockRev(orig_doc1_rev)
        vcr_2 = vectorclock.VectorClockRev(doc2.rev)
        vcr_new = vectorclock.VectorClockRev(doc1.rev)
        self.assertTrue(vcr_new.is_newer(vcr_1))
        self.assertTrue(vcr_new.is_newer(vcr_2))

    def test_resolve_doc_partial_not_winning(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, simple_doc)])
        content3 = '{"key": "valin3"}'
        doc3 = self.make_document(doc1.doc_id, 'third:1', content3)
        self.db.put_doc_if_newer(doc3, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [(doc3.rev, content3),
             (doc1.rev, simple_doc),
             (doc2.rev, nested_doc)])
        self.db.resolve_doc(doc1, [doc2.rev, doc1.rev])
        self.assertTrue(doc1.has_conflicts)
        self.assertGetDoc(self.db, doc1.doc_id, doc3.rev, content3, True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [(doc3.rev, content3),
             (doc1.rev, simple_doc)])

    def test_resolve_doc_partial_winning(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        content3 = '{"key": "valin3"}'
        doc3 = self.make_document(doc1.doc_id, 'third:1', content3)
        self.db.put_doc_if_newer(doc3, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc3.rev, content3),
                                    (doc1.rev, simple_doc),
                                    (doc2.rev, nested_doc)])
        self.db.resolve_doc(doc1, [doc3.rev, doc1.rev])
        self.assertTrue(doc1.has_conflicts)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc1.rev, simple_doc),
                                    (doc2.rev, nested_doc)])

    def test_resolve_doc_with_delete_conflict(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc1)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, None)])
        self.db.resolve_doc(doc2, [doc1.rev, doc2.rev])
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        self.assertGetDoc(self.db, doc2.doc_id, doc2.rev, nested_doc, False)

    def test_resolve_doc_with_delete_to_delete(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc1)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
                                   [(doc2.rev, nested_doc),
                                    (doc1.rev, None)])
        self.db.resolve_doc(doc1, [doc1.rev, doc2.rev])
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        self.assertGetDoc(self.db, doc1.doc_id, doc1.rev, None, False)

    def test_put_doc_if_newer_save_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        # Document is inserted as a conflict
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        state, _ = self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertEqual('conflicted', state)
        # The database was updated
        self.assertGetDoc(self.db, doc1.doc_id, doc2.rev, nested_doc, True)

    def test_force_doc_conflict_supersedes_properly(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', '{"b": 1}')
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        doc3 = self.make_document(doc1.doc_id, 'altalt:1', '{"c": 1}')
        self.db.put_doc_if_newer(doc3, save_conflict=True)
        doc22 = self.make_document(doc1.doc_id, 'alternate:2', '{"b": 2}')
        self.db.put_doc_if_newer(doc22, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:2', doc22.content),
             ('altalt:1', doc3.content),
             (doc1.rev, simple_doc)])

    def test_put_doc_if_newer_save_conflict_was_deleted(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc1)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertTrue(doc2.has_conflicts)
        self.assertGetDoc(self.db, doc1.doc_id, 'alternate:1', nested_doc, True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:1', nested_doc), (doc1.rev, None)])

    def test_put_doc_if_newer_propagates_full_resolution(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        resolved_vcr = vectorclock.VectorClockRev(doc1.rev)
        vcr_2 = vectorclock.VectorClockRev(doc2.rev)
        resolved_vcr.maximize(vcr_2)
        resolved_vcr.increment('alternate')
        doc_resolved = self.make_document(doc1.doc_id, resolved_vcr.as_str(),
                                '{"good": 1}')
        state, _ = self.db.put_doc_if_newer(doc_resolved, save_conflict=True)
        self.assertEqual('inserted', state)
        self.assertFalse(doc_resolved.has_conflicts)
        self.assertGetDocConflicts(self.db, doc1.doc_id, [])
        doc3 = self.db.get_doc(doc1.doc_id)
        self.assertFalse(doc3.has_conflicts)

    def test_put_doc_if_newer_propagates_partial_resolution(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'altalt:1', '{}')
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        doc3 = self.make_document(doc1.doc_id, 'alternate:1', nested_doc)
        self.db.put_doc_if_newer(doc3, save_conflict=True)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:1', nested_doc), ('test:1', simple_doc),
             ('altalt:1', '{}')])
        resolved_vcr = vectorclock.VectorClockRev(doc1.rev)
        vcr_3 = vectorclock.VectorClockRev(doc3.rev)
        resolved_vcr.maximize(vcr_3)
        resolved_vcr.increment('alternate')
        doc_resolved = self.make_document(doc1.doc_id, resolved_vcr.as_str(),
                                          '{"good": 1}')
        state, _ = self.db.put_doc_if_newer(doc_resolved, save_conflict=True)
        self.assertEqual('inserted', state)
        self.assertTrue(doc_resolved.has_conflicts)
        doc4 = self.db.get_doc(doc1.doc_id)
        self.assertTrue(doc4.has_conflicts)
        self.assertGetDocConflicts(self.db, doc1.doc_id,
            [('alternate:2|test:1', '{"good": 1}'), ('altalt:1', '{}')])

    def test_put_doc_if_newer_replica_uid(self):
        doc1 = self.db.create_doc(simple_doc)
        self.db.set_sync_generation('other', 1)
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|other:1',
                                  nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True,
                                 replica_uid='other', replica_gen=2)
        # Conflict vs the current update
        doc2 = self.make_document(doc1.doc_id, doc1.rev + '|third:3',
                                  nested_doc)
        self.assertEqual('conflicted',
            self.db.put_doc_if_newer(doc2, save_conflict=True,
                replica_uid='other', replica_gen=3)[0])
        self.assertEqual(3, self.db.get_sync_generation('other'))

    def test_put_refuses_to_update_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        content2 = '{"key": "altval"}'
        doc2 = self.make_document(doc1.doc_id, 'altrev:1', content2)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDoc(self.db, doc1.doc_id, doc2.rev, content2, True)
        content3 = '{"key": "local"}'
        doc2.content = content3
        self.assertRaises(errors.ConflictedDoc, self.db.put_doc, doc2)

    def test_delete_refuses_for_conflicted(self):
        doc1 = self.db.create_doc(simple_doc)
        doc2 = self.make_document(doc1.doc_id, 'altrev:1', nested_doc)
        self.db.put_doc_if_newer(doc2, save_conflict=True)
        self.assertGetDoc(self.db, doc2.doc_id, doc2.rev, nested_doc, True)
        self.assertRaises(errors.ConflictedDoc, self.db.delete_doc, doc2)


class DatabaseIndexTests(tests.DatabaseBaseTests):

    scenarios = tests.LOCAL_DATABASES_SCENARIOS + tests.C_DATABASE_SCENARIOS

    def test_create_index(self):
        self.db.create_index('test-idx', ['name'])
        self.assertEqual([('test-idx', ['name'])],
                         self.db.list_indexes())

    def test_create_index_evaluates_it(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([doc],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_create_index_after_deleting_document(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.delete_doc(doc2)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([doc],
                         self.db.get_from_index('test-idx', [('value',)]))

    def test_delete_index(self):
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([('test-idx', ['key'])], self.db.list_indexes())
        self.db.delete_index('test-idx')
        self.assertEqual([], self.db.list_indexes())

    def test_create_adds_to_index(self):
        self.db.create_index('test-idx', ['key'])
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_unmatched(self):
        self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual(
            [], self.db.get_from_index('test-idx', [('novalue',)]))

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

    def test_delete_updates_index(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        self.assertEqual(sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', [('value',)])))
        self.db.delete_doc(doc)
        self.assertEqual([doc2],
            self.db.get_from_index('test-idx', [('value',)]))

    def test_get_from_index_illegal_number_of_entries(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [()])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('v1',)])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('v1', 'v2', 'v3')])

    def test_get_from_index_illegal_wildcard_order(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('*', 'v2')])

    def test_get_from_index_illegal_glob_after_wildcard(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('*', 'v*')])

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

    def test_put_updates_when_adding_key(self):
        doc = self.db.create_doc("{}")
        self.db.create_index('test-idx', ['key'])
        self.assertEqual([],
            self.db.get_from_index('test-idx', [('*',)]))
        doc.content = simple_doc
        self.db.put_doc(doc)
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('*',)]))

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

    def test_nested_index(self):
        doc = self.db.create_doc(nested_doc)
        self.db.create_index('test-idx', ['sub.doc'])
        self.assertEqual([doc],
            self.db.get_from_index('test-idx', [('underneath',)]))
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual(
            sorted([doc, doc2]),
            sorted(self.db.get_from_index('test-idx', [('underneath',)])))

    def test_nested_nonexistent(self):
        doc = self.db.create_doc(nested_doc)
        # sub exists, but sub.foo does not:
        self.db.create_index('test-idx', ['sub.foo'])
        self.assertEqual([], self.db.get_from_index('test-idx', [('*',)]))

    def test_nested_nonexistent2(self):
        doc = self.db.create_doc(nested_doc)
        # sub exists, but sub.foo does not:
        self.db.create_index('test-idx', ['sub.foo.bar.baz.qux.fnord'])
        self.assertEqual([], self.db.get_from_index('test-idx', [('*',)]))

    def test_index_list1(self):
        self.db.create_index("index", ["name"])
        content = '{"name": ["foo", "bar"]}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("bar", )])
        self.assertEqual([doc], rows)

    def test_index_list2(self):
        self.db.create_index("index", ["name"])
        content = '{"name": ["foo", "bar"]}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("foo", )])
        self.assertEqual([doc], rows)

    def test_get_from_index_case_sensitive(self):
        self.db.create_index('test-idx', ['key'])
        doc1 = self.db.create_doc(simple_doc)
        self.assertEqual([], self.db.get_from_index('test-idx', [('V*',)]))
        self.assertEqual([doc1],
                         self.db.get_from_index('test-idx', [('v*',)]))

    def test_get_from_index_illegal_glob_before_value(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('v*', 'v2')])

    def test_get_from_index_illegal_glob_after_glob(self):
        self.db.create_index('test-idx', ['k1', 'k2'])
        self.assertRaises(errors.InvalidValueForIndex,
            self.db.get_from_index, 'test-idx', [('*', 'v*')])

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

    def test_get_from_index_with_lower(self):
        self.db.create_index("index", ["lower(name)"])
        content = '{"name": "Foo"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("foo", )])
        self.assertEqual([doc], rows)

    def test_get_from_index_with_lower_matches_same_case(self):
        self.db.create_index("index", ["lower(name)"])
        content = '{"name": "foo"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("foo", )])
        self.assertEqual([doc], rows)

    def test_index_lower_doesnt_match_different_case(self):
        self.db.create_index("index", ["lower(name)"])
        content = '{"name": "Foo"}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("Foo", )])
        self.assertEqual([], rows)

    def test_index_lower_doesnt_match_other_index(self):
        self.db.create_index("index", ["lower(name)"])
        self.db.create_index("other_index", ["name"])
        content = '{"name": "Foo"}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("Foo", )])
        self.assertEqual(0, len(rows))

    def test_index_split_words_match_first(self):
        self.db.create_index("index", ["split_words(name)"])
        content = '{"name": "foo bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("foo", )])
        self.assertEqual([doc], rows)

    def test_index_split_words_match_second(self):
        self.db.create_index("index", ["split_words(name)"])
        content = '{"name": "foo bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("bar", )])
        self.assertEqual([doc], rows)

    def test_index_split_words_match_both(self):
        self.db.create_index("index", ["split_words(name)"])
        content = '{"name": "foo foo"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("foo", )])
        self.assertEqual([doc], rows)

    def test_index_split_words_double_space(self):
        self.db.create_index("index", ["split_words(name)"])
        content = '{"name": "foo  bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("bar", )])
        self.assertEqual([doc], rows)

    def test_index_split_words_leading_space(self):
        self.db.create_index("index", ["split_words(name)"])
        content = '{"name": " foo bar"}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("foo", )])
        self.assertEqual([doc], rows)

    def test_index_split_words_trailing_space(self):
        self.db.create_index("index", ["split_words(name)"])
        content = '{"name": "foo bar "}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("bar", )])
        self.assertEqual([doc], rows)

    def test_get_from_index_with_number(self):
        self.db.create_index("index", ["number(foo, 5)"])
        content = '{"foo": 12}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("00012", )])
        self.assertEqual([doc], rows)

    def test_get_from_index_with_number_bigger_than_padding(self):
        self.db.create_index("index", ["number(foo, 5)"])
        content = '{"foo": 123456}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("123456", )])
        self.assertEqual([doc], rows)

    def test_number_mapping_ignores_non_numbers(self):
        self.db.create_index("index", ["number(foo, 5)"])
        content = '{"foo": 56}'
        doc1 = self.db.create_doc(content)
        content = '{"foo": "this is not a maigret painting"}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("*", )])
        self.assertEqual([doc1], rows)

    def test_get_from_index_with_bool(self):
        self.db.create_index("index", ["bool(foo)"])
        content = '{"foo": true}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("1", )])
        self.assertEqual([doc], rows)

    def test_get_from_index_with_bool_false(self):
        self.db.create_index("index", ["bool(foo)"])
        content = '{"foo": false}'
        doc = self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("0", )])
        self.assertEqual([doc], rows)

    def test_get_from_index_with_non_bool(self):
        self.db.create_index("index", ["bool(foo)"])
        content = '{"foo": 42}'
        self.db.create_doc(content)
        rows = self.db.get_from_index("index", [("*", )])
        self.assertEqual([], rows)

    def test_get_index_keys_from_index(self):
        self.db.create_index('test-idx', ['key'])
        content1 = '{"key": "value1"}'
        content2 = '{"key": "value2"}'
        content3 = '{"key": "value2"}'
        self.db.create_doc(content1)
        self.db.create_doc(content2)
        self.db.create_doc(content3)
        self.assertEqual(
            ['value1', 'value2'],
            sorted(self.db.get_index_keys('test-idx')))


class PyDatabaseIndexTests(tests.DatabaseBaseTests):

    def test_sync_exchange_updates_indexes(self):
        doc = self.db.create_doc(simple_doc)
        self.db.create_index('test-idx', ['key'])
        new_content = '{"key": "altval"}'
        other_rev = 'test:1|z:2'
        st = self.db.get_sync_target()

        def ignore(doc_id, doc_rev, doc):
            pass

        doc_other = self.make_document(doc.doc_id, other_rev, new_content)
        docs_by_gen = [(doc_other, 10)]
        st.sync_exchange(
            docs_by_gen, 'other-replica', last_known_generation=0,
            return_doc_cb=ignore)
        self.assertGetDoc(self.db, doc.doc_id, other_rev, new_content, False)
        self.assertEqual([doc_other],
                         self.db.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db.get_from_index('test-idx', [('value',)]))


# Use a custom loader to apply the scenarios at load time.
load_tests = tests.load_with_scenarios
