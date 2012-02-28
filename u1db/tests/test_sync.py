# Copyright 2011-2012 Canonical Ltd.
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

"""The Synchronization class for U1DB."""

import os
from wsgiref import simple_server

from u1db import (
    errors,
    sync,
    tests,
    )
from u1db.backends import (
    inmemory,
    )
from u1db.remote import (
    http_target,
    )

from u1db.tests.test_remote_sync_target import (
    http_server_def,
    oauth_http_server_def,
    )

simple_doc = tests.simple_doc
nested_doc = tests.nested_doc


def _make_local_db_and_target(test):
    db = test.create_database('test')
    st = db.get_sync_target()
    return db, st


def _make_local_db_and_http_target(test, path='test'):
    test.startServer()
    db = test.request_state._create_database(os.path.basename(path))
    st = http_target.HTTPSyncTarget.connect(test.getURL(path))
    return db, st


def _make_local_db_and_oauth_http_target(test):
    db, st = _make_local_db_and_http_target(test, '~/test')
    st.set_oauth_credentials(tests.consumer1.key, tests.consumer1.secret,
                             tests.token1.key, tests.token1.secret)
    return db, st


target_scenarios = [
    ('local', {'create_db_and_target': _make_local_db_and_target,
               'make_document': tests.create_doc}),
    ('http', {'create_db_and_target': _make_local_db_and_http_target,
              'make_document': tests.create_doc,
              'server_def': http_server_def}),
    ('oauth_http', {'create_db_and_target':
                    _make_local_db_and_oauth_http_target,
                    'make_document': tests.create_doc,
                    'server_def': oauth_http_server_def}),
    ]


# An explanation of the transitional setup for the following classes
# We want to be running all tests against all back ends. However, until all
# backends pass the tests, we need a way to allow them to run against some of
# them.
# 1) We want to pull out the common logic for how to set up the database and
#    server we will be testing against, and how it integrates with the unittest
#    framework. However, our test loader will automatically run any 'test_foo'
#    functions, even though we haven't permutated the class for a given
#    scenario.
# 2) We permute every backend against every server. However, the C backend
#    cannot talk HTTP or HTTP+OAuth yet. So we need to restrict its
#    parameterization, as well as its tests.
# 3) So we end up with 1 class that encapsulates the unittest infrastructure,
#    but does not contain any test_* functions (SyncTargetTestSetup). And
#    another class that holds common tests that are run against everything
#    (DatabaseSyncTargetTests), a class for the C backend and a class for all
#    the python backends.
# 4) Eventually, all this will be pull back into a single class that holds
#    everything (DatabaseSyncTargetTests) with every backend permuted against
#    every server.
class SyncTargetTestSetup(tests.DatabaseBaseTests, tests.TestCaseWithServer):

    whitebox = True

    def setUp(self):
        super(SyncTargetTestSetup, self).setUp()
        self.db, self.st = self.create_db_and_target(self)
        self.other_changes = []

    def tearDown(self):
        # We delete them explicitly, so that connections are cleanly closed
        del self.st
        self.db.close()
        del self.db
        super(SyncTargetTestSetup, self).tearDown()


class DatabaseSyncTargetTests(object):

    def receive_doc(self, doc, gen):
        self.other_changes.append((doc.doc_id, doc.rev, doc.content, gen))

    def test_get_sync_target(self):
        self.assertIsNot(None, self.st)

    def test_get_sync_info(self):
        self.assertEqual(('test', 0, 0), self.st.get_sync_info('other'))

    def test_create_doc_updates_sync_info(self):
        self.assertEqual(('test', 0, 0), self.st.get_sync_info('other'))
        doc = self.db.create_doc(simple_doc)
        self.assertEqual(('test', 1, 0), self.st.get_sync_info('other'))

    def test_record_sync_info(self):
        self.assertEqual(('test', 0, 0), self.st.get_sync_info('replica'))
        self.st.record_sync_info('replica', 10)
        self.assertEqual(('test', 0, 10), self.st.get_sync_info('replica'))

    def test_sync_exchange(self):
        docs_by_gen = [
            (self.make_document('doc-id', 'replica:1', simple_doc), 10)]
        new_gen = self.st.sync_exchange(docs_by_gen, 'replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertGetDoc(self.db, 'doc-id', 'replica:1', simple_doc, False)
        self.assertEqual(['doc-id'], self.db._get_transaction_log())
        self.assertEqual(([], 1), (self.other_changes, new_gen))
        self.assertEqual(10, self.st.get_sync_info('replica')[-1])



class CDatabaseSyncTargetTests(SyncTargetTestSetup, DatabaseSyncTargetTests):

    scenarios = tests.multiply_scenarios(tests.C_DATABASE_SCENARIOS,
        target_scenarios[:1]) # We don't yet test http or oauth_http



class PyDatabaseSyncTargetTests(SyncTargetTestSetup, DatabaseSyncTargetTests):

    scenarios = tests.multiply_scenarios(tests.DatabaseBaseTests.scenarios,
                                         target_scenarios)

    # whitebox true means self.db is the actual local db object
    # against which the sync is performed
    whitebox = True

    def test_sync_exchange_push_many(self):
        docs_by_gen = [
            (self.make_document('doc-id', 'replica:1', simple_doc), 10),
            (self.make_document('doc-id2', 'replica:1', nested_doc), 11)]
        new_gen = self.st.sync_exchange(docs_by_gen, 'replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertGetDoc(self.db, 'doc-id', 'replica:1', simple_doc, False)
        self.assertGetDoc(self.db, 'doc-id2', 'replica:1', nested_doc, False)
        self.assertEqual(['doc-id', 'doc-id2'], self.db._get_transaction_log())
        self.assertEqual(([], 2), (self.other_changes, new_gen))
        self.assertEqual(11, self.st.get_sync_info('replica')[-1])

    def test_sync_exchange_refuses_conflicts(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        new_doc = '{"key": "altval"}'
        docs_by_gen = [
            (self.make_document(doc.doc_id, 'replica:1', new_doc), 10)]
        new_gen = self.st.sync_exchange(docs_by_gen, 'replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        self.assertEqual(([(doc.doc_id, doc.rev, simple_doc, 1)], 1),
                         (self.other_changes, new_gen))
        if self.whitebox:
            self.assertEqual(self.db._last_exchange_log['return'],
                             {'last_gen': 1, 'docs': [(doc.doc_id, doc.rev)]})

    def test_sync_exchange_ignores_convergence(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        docs_by_gen = [
            (self.make_document(doc.doc_id, doc.rev, simple_doc), 10)]
        new_gen = self.st.sync_exchange(docs_by_gen, 'replica',
                                        last_known_generation=1,
                                        return_doc_cb=self.receive_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        self.assertEqual(([], 1), (self.other_changes, new_gen))

    def test_sync_exchange_returns_new_docs(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        new_gen = self.st.sync_exchange([], 'other-replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        self.assertEqual(([(doc.doc_id, doc.rev, simple_doc, 1)], 1),
                         (self.other_changes, new_gen))
        if self.whitebox:
            self.assertEqual(self.db._last_exchange_log['return'],
                             {'last_gen': 1, 'docs': [(doc.doc_id, doc.rev)]})

    def test_sync_exchange_returns_many_new_docs(self):
        doc = self.db.create_doc(simple_doc)
        doc2 = self.db.create_doc(nested_doc)
        self.assertEqual([doc.doc_id, doc2.doc_id],
                         self.db._get_transaction_log())
        new_gen = self.st.sync_exchange([], 'other-replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertEqual([doc.doc_id, doc2.doc_id],
                         self.db._get_transaction_log())
        self.assertEqual(([(doc.doc_id, doc.rev, simple_doc, 1),
                           (doc2.doc_id, doc2.rev, nested_doc, 2)], 2),
                         (self.other_changes, new_gen))
        if self.whitebox:
            self.assertEqual(self.db._last_exchange_log['return'],
                             {'last_gen': 2, 'docs': [(doc.doc_id, doc.rev),
                                                      (doc2.doc_id, doc2.rev)]})

    def test_sync_exchange_getting_newer_docs(self):
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        new_doc = '{"key": "altval"}'
        docs_by_gen = [
            (self.make_document(doc.doc_id, 'test:1|z:2', new_doc), 10)]
        new_gen = self.st.sync_exchange(docs_by_gen, 'other-replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertEqual([doc.doc_id, doc.doc_id],
                         self.db._get_transaction_log())
        self.assertEqual(([], 2), (self.other_changes, new_gen))

    def test_sync_exchange_with_concurrent_updates(self):
        if not self.whitebox:
            self.skipTest("requires to be able to monkeypatch the target db")
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        orig_wc = self.db.whats_changed
        def after_whatschanged(*args, **kwargs):
            val = orig_wc(*args, **kwargs)
            self.db.create_doc('{"new": "doc"}')
            return val
        self.db.whats_changed = after_whatschanged
        new_doc = '{"key": "altval"}'
        docs_by_gen = [
            (self.make_document(doc.doc_id, 'test:1|z:2', new_doc), 10)]
        new_gen = self.st.sync_exchange(docs_by_gen, 'other-replica',
                                        last_known_generation=0,
                                        return_doc_cb=self.receive_doc)
        self.assertEqual(([], 2), (self.other_changes, new_gen))

    def test_sync_exchange_detect_incomplete_exchange(self):
        if not self.whitebox:
            self.skipTest("requires to be able to monkeypatch the target db")
        # suppress traceback printing in the wsgiref server
        self.patch(simple_server.ServerHandler,
                   'log_exception', lambda h, exc_info: None)
        doc = self.db.create_doc(simple_doc)
        self.assertEqual([doc.doc_id], self.db._get_transaction_log())
        class Fail(Exception):
            pass
        def exploding_get_docs(doc_ids, check_for_conflicts):
            raise Fail
        self.db.get_docs = exploding_get_docs
        self.assertRaises((Fail, errors.BrokenSyncStream),
                          self.st.sync_exchange, [], 'other-replica',
                          last_known_generation=0,
                          return_doc_cb=self.receive_doc)


class DatabaseSyncTests(tests.DatabaseBaseTests):

    def setUp(self):
        super(DatabaseSyncTests, self).setUp()
        self.db1 = self.create_database('test1')
        self.db2 = self.create_database('test2')

    def sync(self, db_source, db_target):
        return sync.Synchronizer(db_source, db_target.get_sync_target()).sync()

    def test_sync_tracks_db_generation_of_other(self):
        self.assertEqual(0, self.sync(self.db1, self.db2))
        self.assertEqual(0, self.db1.get_sync_generation('test2'))
        self.assertEqual(0, self.db2.get_sync_generation('test1'))
        self.assertEqual({'receive': {'docs': [], 'last_known_gen': 0},
                          'return': {'docs': [], 'last_gen': 0}},
                         self.db2._last_exchange_log)

    def test_sync_puts_changes(self):
        doc = self.db1.create_doc(simple_doc)
        self.assertEqual(1, self.sync(self.db1, self.db2))
        self.assertGetDoc(self.db2, doc.doc_id, doc.rev, simple_doc, False)
        self.assertEqual(1, self.db1.get_sync_generation('test2'))
        self.assertEqual(1, self.db2.get_sync_generation('test1'))
        self.assertEqual({'receive': {'docs': [(doc.doc_id, doc.rev)],
                                      'source_uid': 'test1',
                                      'source_gen': 1, 'last_known_gen': 0},
                          'return': {'docs': [], 'last_gen': 1}},
                         self.db2._last_exchange_log)

    def test_sync_pulls_changes(self):
        doc = self.db2.create_doc(simple_doc)
        self.db1.create_index('test-idx', ['key'])
        self.assertEqual(0, self.sync(self.db1, self.db2))
        self.assertGetDoc(self.db1, doc.doc_id, doc.rev, simple_doc, False)
        self.assertEqual(1, self.db1.get_sync_generation('test2'))
        self.assertEqual(1, self.db2.get_sync_generation('test1'))
        self.assertEqual({'receive': {'docs': [], 'last_known_gen': 0},
                          'return': {'docs': [(doc.doc_id, doc.rev)],
                                     'last_gen': 1}},
                         self.db2._last_exchange_log)
        self.assertEqual([doc],
                         self.db1.get_from_index('test-idx', [('value',)]))

    def test_sync_pulling_doesnt_update_other_if_changed(self):
        doc = self.db2.create_doc(simple_doc)
        # Right after we call c2._sync_exchange, we update our local database
        # with a new record. When we finish synchronizing, we can notice that
        # something locally was updated, and we cannot tell c2 our new updated
        # generation
        orig = self.db1.put_doc_if_newer
        once = [None]
        def after_put_doc_if_newer(*args, **kwargs):
            result = orig(*args, **kwargs)
            if once:
                self.db1.create_doc(simple_doc)
                once.pop()
            return result
        self.db1.put_doc_if_newer = after_put_doc_if_newer
        self.assertEqual(0, self.sync(self.db1, self.db2))
        self.assertEqual({'receive': {'docs': [], 'last_known_gen': 0},
                          'return': {'docs': [(doc.doc_id, doc.rev)],
                                     'last_gen': 1}},
                         self.db2._last_exchange_log)
        self.assertEqual(1, self.db1.get_sync_generation('test2'))
        # c2 should not have gotten a '_record_sync_info' call, because the
        # local database had been updated more than just by the messages
        # returned from c2.
        self.assertEqual(0, self.db2.get_sync_generation('test1'))

    def test_sync_ignores_convergence(self):
        doc = self.db1.create_doc(simple_doc)
        self.db3 = self.create_database('test3')
        self.assertEqual(1, self.sync(self.db1, self.db3))
        self.assertEqual(0, self.sync(self.db2, self.db3))
        self.assertEqual(1, self.sync(self.db1, self.db2))
        self.assertEqual({'receive': {'docs': [(doc.doc_id, doc.rev)],
                                      'source_uid': 'test1',
                                      'source_gen': 1, 'last_known_gen': 0},
                          'return': {'docs': [], 'last_gen': 1}},
                         self.db2._last_exchange_log)

    def test_sync_ignores_superseded(self):
        doc = self.db1.create_doc(simple_doc)
        doc_rev1 = doc.rev
        self.db3 = self.create_database('test3')
        self.sync(self.db1, self.db3)
        self.sync(self.db2, self.db3)
        new_content = '{"key": "altval"}'
        doc.content = new_content
        self.db1.put_doc(doc)
        doc_rev2 = doc.rev
        self.sync(self.db2, self.db1)
        self.assertEqual({'receive': {'docs': [(doc.doc_id, doc_rev1)],
                                      'source_uid': 'test2',
                                      'source_gen': 1, 'last_known_gen': 0},
                          'return': {'docs': [(doc.doc_id, doc_rev2)],
                                     'last_gen': 2}},
                         self.db1._last_exchange_log)
        self.assertGetDoc(self.db1, doc.doc_id, doc_rev2, new_content, False)

    def test_sync_sees_remote_conflicted(self):
        doc1 = self.db1.create_doc(simple_doc)
        doc_id = doc1.doc_id
        doc1_rev = doc1.rev
        self.db1.create_index('test-idx', ['key'])
        new_doc = '{"key": "altval"}'
        doc2 = self.db2.create_doc(new_doc, doc_id=doc_id)
        doc2_rev = doc2.rev
        self.assertEqual([doc1.doc_id], self.db1._get_transaction_log())
        self.sync(self.db1, self.db2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc1_rev)],
                                      'source_uid': 'test1',
                                      'source_gen': 1, 'last_known_gen': 0},
                          'return': {'docs': [(doc_id, doc2_rev)],
                                     'last_gen': 1}},
                         self.db2._last_exchange_log)
        self.assertEqual([doc_id, doc_id], self.db1._get_transaction_log())
        self.assertGetDoc(self.db1, doc_id, doc2_rev, new_doc, True)
        self.assertGetDoc(self.db2, doc_id, doc2_rev, new_doc, False)
        self.assertEqual([doc2],
                         self.db1.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))

    def test_sync_sees_remote_delete_conflicted(self):
        doc1 = self.db1.create_doc(simple_doc)
        doc_id = doc1.doc_id
        self.db1.create_index('test-idx', ['key'])
        self.sync(self.db1, self.db2)
        doc2 = self.make_document(doc1.doc_id, doc1.rev, doc1.content)
        new_doc = '{"key": "altval"}'
        doc1.content = new_doc
        self.db1.put_doc(doc1)
        self.db2.delete_doc(doc2)
        self.assertEqual([doc_id, doc_id], self.db1._get_transaction_log())
        self.sync(self.db1, self.db2)
        self.assertEqual({'receive': {'docs': [(doc_id, doc1.rev)],
                                      'source_uid': 'test1',
                                      'source_gen': 2, 'last_known_gen': 1},
                          'return': {'docs': [(doc_id, doc2.rev)],
                                     'last_gen': 2}},
                         self.db2._last_exchange_log)
        self.assertEqual([doc_id, doc_id, doc_id],
                         self.db1._get_transaction_log())
        self.assertGetDoc(self.db1, doc_id, doc2.rev, None, True)
        self.assertGetDoc(self.db2, doc_id, doc2.rev, None, False)
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))

    def test_sync_local_race_conflicted(self):
        doc = self.db1.create_doc(simple_doc)
        doc_id = doc.doc_id
        doc1_rev = doc.rev
        self.db1.create_index('test-idx', ['key'])
        self.sync(self.db1, self.db2)
        content1 = '{"key": "localval"}'
        content2 = '{"key": "altval"}'
        doc.content = content2
        self.db2.put_doc(doc)
        doc2_rev2 = doc.rev
        # Monkey patch so that after the local client has determined recent
        # changes, we get another one, before sync finishes.
        orig_wc = self.db1.whats_changed
        def after_whatschanged(*args, **kwargs):
            val = orig_wc(*args, **kwargs)
            doc = self.make_document(doc_id, doc1_rev, content1)
            self.db1.put_doc(doc)
            return val
        self.db1.whats_changed = after_whatschanged
        self.sync(self.db1, self.db2)
        self.assertGetDoc(self.db1, doc_id, doc2_rev2, content2, True)
        self.assertEqual([doc],
                         self.db1.get_from_index('test-idx', [('altval',)]))
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))
        self.assertEqual([], self.db1.get_from_index('test-idx',
                                                     [('localval',)]))

    def test_sync_propagates_deletes(self):
        doc1 = self.db1.create_doc(simple_doc)
        doc_id = doc1.doc_id
        doc1_rev = doc1.rev
        self.db1.create_index('test-idx', ['key'])
        self.sync(self.db1, self.db2)
        self.db2.create_index('test-idx', ['key'])
        self.db3 = self.create_database('test3')
        self.sync(self.db1, self.db3)
        self.db1.delete_doc(doc1)
        deleted_rev = doc1.rev
        self.sync(self.db1, self.db2)
        self.assertEqual({'receive': {'docs': [(doc_id, deleted_rev)],
                                      'source_uid': 'test1',
                                      'source_gen': 2, 'last_known_gen': 1},
                          'return': {'docs': [], 'last_gen': 2}},
                         self.db2._last_exchange_log)
        self.assertGetDoc(self.db1, doc_id, deleted_rev, None, False)
        self.assertGetDoc(self.db2, doc_id, deleted_rev, None, False)
        self.assertEqual([], self.db1.get_from_index('test-idx', [('value',)]))
        self.assertEqual([], self.db2.get_from_index('test-idx', [('value',)]))
        self.sync(self.db2, self.db3)
        self.assertEqual({'receive': {'docs': [(doc_id, deleted_rev)],
                                      'source_uid': 'test2',
                                      'source_gen': 2, 'last_known_gen': 0},
                          'return': {'docs': [], 'last_gen': 2}},
                         self.db3._last_exchange_log)
        self.assertGetDoc(self.db3, doc_id, deleted_rev, None, False)

    def test_sync_propagates_resolution(self):
        doc1 = self.db1.create_doc('{"a": 1}', doc_id='the-doc')
        db3 = self.create_database('test3')
        self.sync(self.db2, self.db1)
        self.sync(db3, self.db1)
        # update on 2
        doc2 = self.make_document('the-doc', doc1.rev, '{"a": 2}')
        self.db2.put_doc(doc2)
        self.sync(self.db2, db3)
        self.assertEqual(db3.get_doc('the-doc').rev, doc2.rev)
        # update on 1
        doc1.content = '{"a": 3}'
        self.db1.put_doc(doc1)
        # conflicts
        self.sync(self.db2, self.db1)
        self.sync(db3, self.db1)
        self.assertTrue(self.db2.get_doc('the-doc').has_conflicts)
        self.assertTrue(db3.get_doc('the-doc').has_conflicts)
        # resolve
        conflicts = self.db2.get_doc_conflicts('the-doc')
        doc4 = self.make_document('the-doc', None, '{"a": 4}')
        revs = [doc.rev for doc in conflicts]
        self.db2.resolve_doc(doc4, revs)
        doc2 = self.db2.get_doc('the-doc')
        self.assertEqual(doc4.content, doc2.content)
        self.assertFalse(doc2.has_conflicts)
        self.sync(self.db2, db3)
        doc3 = db3.get_doc('the-doc')
        self.assertEqual(doc4.content, doc3.content)
        self.assertFalse(doc3.has_conflicts)

    def test_sync_supersedes_conflicts(self):
        db3 = self.create_database('test3')
        doc1 = self.db1.create_doc('{"a": 1}', doc_id='the-doc')
        doc2 = self.db2.create_doc('{"b": 1}', doc_id='the-doc')
        doc3 = db3.create_doc('{"c": 1}', doc_id='the-doc')
        self.sync(db3, self.db1)
        self.sync(db3, self.db2)
        self.assertEqual(3, len(db3.get_doc_conflicts('the-doc')))
        doc1.content = '{"a": 2}'
        self.db1.put_doc(doc1)
        self.sync(db3, self.db1)
        # original doc1 should have been removed from conflicts
        self.assertEqual(3, len(db3.get_doc_conflicts('the-doc')))

    def test_put_refuses_to_update_conflicted(self):
        doc1 = self.db1.create_doc(simple_doc)
        doc_id = doc1.doc_id
        content1 = '{"key": "altval"}'
        doc2 = self.db2.create_doc(content1, doc_id=doc_id)
        self.sync(self.db1, self.db2)
        self.assertGetDoc(self.db1, doc_id, doc2.rev, content1, True)
        content2 = '{"key": "local"}'
        doc2.content = content2
        self.assertRaises(errors.ConflictedDoc, self.db1.put_doc, doc2)

    def test_delete_refuses_for_conflicted(self):
        doc1 = self.db1.create_doc(simple_doc)
        doc2 = self.db2.create_doc(nested_doc, doc_id=doc1.doc_id)
        self.sync(self.db1, self.db2)
        self.assertGetDoc(self.db1, doc2.doc_id, doc2.rev, nested_doc, True)
        self.assertRaises(errors.ConflictedDoc, self.db1.delete_doc, doc2)

    def test_get_doc_conflicts_unconflicted(self):
        doc = self.db1.create_doc(simple_doc)
        self.assertEqual([], self.db1.get_doc_conflicts(doc.doc_id))

    def test_get_doc_conflicts_no_such_id(self):
        self.assertEqual([], self.db1.get_doc_conflicts('doc-id'))

    def test_get_doc_conflicts(self):
        doc1 = self.db1.create_doc(simple_doc)
        content1 = '{"key": "altval"}'
        doc2 = self.db2.create_doc(content1, doc_id=doc1.doc_id)
        self.sync(self.db1, self.db2)
        self.assertGetDocConflicts(self.db1, doc1.doc_id,
            [(doc2.rev, content1), (doc1.rev, simple_doc)])


class TestDbSync(tests.TestCaseWithServer):
    """Test db.sync remote sync shortcut"""

    server_def = staticmethod(http_server_def)

    def setUp(self):
        super(TestDbSync, self).setUp()
        self.startServer()
        self.db = inmemory.InMemoryDatabase('test1')
        self.db2 = self.request_state._create_database('test2.db')

    def test_db_sync(self):
        doc1 = self.db.create_doc(tests.simple_doc)
        doc2 = self.db2.create_doc(tests.nested_doc)
        db2_url = self.getURL('test2.db')
        self.db.sync(db2_url)
        self.assertGetDoc(self.db2, doc1.doc_id, doc1.rev, tests.simple_doc,
                          False)
        self.assertGetDoc(self.db, doc2.doc_id, doc2.rev, tests.nested_doc,
                          False)


class TestRemoteSyncIntegration(tests.TestCaseWithServer):
    """Integration tests for the most common sync scenario local -> remote"""

    server_def = staticmethod(http_server_def)

    def setUp(self):
        super(TestRemoteSyncIntegration, self).setUp()
        self.startServer()
        self.db1 = inmemory.InMemoryDatabase('test1')
        self.db2 = self.request_state._create_database('test2')

    def test_sync_tracks_generations_incrementally(self):
        doc11 = self.db1.create_doc('{"a": 1}')
        doc12 = self.db1.create_doc('{"a": 2}')
        doc21 = self.db2.create_doc('{"b": 1}')
        doc22 = self.db2.create_doc('{"b": 2}')
        #sanity
        self.assertEqual(2, len(self.db1._get_transaction_log()))
        self.assertEqual(2, len(self.db2._get_transaction_log()))
        progress1 = []
        progress2 = []
        _set_sync_generation1 = self.db1._set_sync_generation
        def set_sync_generation_witness1(other_uid, other_gen):
            progress1.append((other_uid, other_gen,
                              self.db1._get_transaction_log()[2:]))
            _set_sync_generation1(other_uid, other_gen)
        self.patch(self.db1, '_set_sync_generation',
                   set_sync_generation_witness1)

        _set_sync_generation2 = self.db2._set_sync_generation
        def set_sync_generation_witness2(other_uid, other_gen):
            progress2.append((other_uid, other_gen,
                              self.db2._get_transaction_log()[2:]))
            _set_sync_generation2(other_uid, other_gen)
        self.patch(self.db2, '_set_sync_generation',
                   set_sync_generation_witness2)

        db2_url = self.getURL('test2')
        self.db1.sync(db2_url)

        self.assertEqual([('test2', 1, [doc21.doc_id]),
                          ('test2', 2, [doc21.doc_id, doc22.doc_id]),
                          ('test2', 4, [doc21.doc_id, doc22.doc_id])],
                         progress1)
        self.assertEqual([('test1', 1, [doc11.doc_id]),
                          ('test1', 2, [doc11.doc_id, doc12.doc_id]),
                          ('test1', 4, [doc11.doc_id, doc12.doc_id])],
                         progress2)


load_tests = tests.load_with_scenarios
