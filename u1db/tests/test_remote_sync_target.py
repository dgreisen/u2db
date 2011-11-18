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

"""Tests for the RemoteSyncTarget"""

import os

from u1db import (
    tests,
    )
from u1db.remote import (
    sync_target,
    )
from u1db.backends import (
    sqlite_backend,
    )


class TestRemoteSyncTarget(tests.TestCaseWithSyncServer):

    def getSyncTarget(self, path=None):
        if self.server is None:
            self.startServer()
        return sync_target.RemoteSyncTarget.connect(self.getURL(path))

    def test_connect(self):
        self.startServer()
        url = self.getURL()
        remote_target = sync_target.RemoteSyncTarget.connect(url)
        self.assertEqual(url, remote_target._url.geturl())
        self.assertIs(None, remote_target._conn)

    def test_parse_url(self):
        remote_target = sync_target.RemoteSyncTarget('u1db://127.0.0.1:12345/')
        self.assertEqual('u1db', remote_target._url.scheme)
        self.assertEqual('127.0.0.1', remote_target._url.hostname)
        self.assertEqual(12345, remote_target._url.port)
        self.assertEqual('/', remote_target._url.path)

    def test_no_sync_exchange_object(self):
        remote_target = self.getSyncTarget()
        self.assertEqual(None, remote_target.get_sync_exchange())

    def test__ensure_connection(self):
        remote_target = self.getSyncTarget()
        self.assertIs(None, remote_target._conn)
        remote_target._ensure_connection()
        self.assertIsNot(None, remote_target._conn)
        c = remote_target._conn
        remote_target._ensure_connection()
        self.assertIs(c, remote_target._conn)
        self.assertIsNot(None, remote_target._client)

    def test_get_sync_info(self):
        self.startServer()
        db = self.request_state._create_database('test')
        db.set_sync_generation('other-id', 1)
        remote_target = self.getSyncTarget('test')
        self.assertEqual(('test', 0, 1),
                         remote_target.get_sync_info('other-id'))

    def test_record_sync_info(self):
        self.startServer()
        db = self.request_state._create_database('test')
        remote_target = self.getSyncTarget('test')
        remote_target.record_sync_info('other-id', 2)
        self.assertEqual(db.get_sync_generation('other-id'), 2)

    def test_sync_exchange_send(self):
        self.startServer()
        db = self.request_state._create_database('test')
        remote_target = self.getSyncTarget('test')
        other_docs = []
        def receive_doc(doc_id, doc_rev, doc):
            other_docs.append((doc_id, doc_id, doc))
        new_gen = remote_target.sync_exchange(
                        [('doc-here', 'replica:1', {'value': 'here'})],
                        'replica', from_replica_generation=10,
                        last_known_generation=0, return_doc_cb=receive_doc)
        self.assertEqual(1, new_gen)
        self.assertGetDoc(db, 'doc-here', 'replica:1', {'value': 'here'}, False)

    def test_sync_exchange_receive(self):
        self.startServer()
        db = self.request_state._create_database('test')
        doc = db.create_doc({'value': 'there'})
        remote_target = self.getSyncTarget('test')
        other_docs = []
        def receive_doc(doc_id, doc_rev, doc):
            other_docs.append((doc_id, doc_rev, doc))
        new_gen = remote_target.sync_exchange(
                        [],
                        'replica', from_replica_generation=10,
                        last_known_generation=0, return_doc_cb=receive_doc)
        self.assertEqual(1, new_gen)
        self.assertEqual([(doc.doc_id, doc.rev, {'value': 'there'})],
                         other_docs)
