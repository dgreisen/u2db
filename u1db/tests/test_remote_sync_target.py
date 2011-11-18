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

"""Tests for the remote sync targets"""

import os
from wsgiref import simple_server
#from paste import httpserver

from u1db import (
    tests,
    )
from u1db.remote import (
    sync_server,
    sync_target,
    http_app,
    http_target
    )
from u1db.backends import (
    sqlite_backend,
    )


def remote_server_def():
    return (sync_server.TCPSyncServer, sync_server.TCPSyncRequestHandler,
            "force_shutdown", "u1db")

def http_server_def():
    def make_server(host_port, handler, state):
        application = http_app.HTTPApp(state)
        srv = simple_server.WSGIServer(host_port, handler)
        srv.set_app(application)
        #srv = httpserver.WSGIServerBase(application,
        #                                host_port,
        #                                handler
        #                                )
        return srv
    class req_handler(simple_server.WSGIRequestHandler):
        def log_request(*args):
            pass # suppress
    #rh = httpserver.WSGIHandler
    return make_server, req_handler, "shutdown", "http"


class TestRemoteSyncTarget(tests.TestCaseWithServer):

    server_def = staticmethod(remote_server_def)

    def test_connect(self):
        self.startServer()
        url = self.getURL()
        remote_target = sync_target.RemoteSyncTarget(url)
        self.assertEqual(url, remote_target._url.geturl())
        self.assertIs(None, remote_target._client)

    def test__ensure_connection(self):
        self.startServer()
        remote_target = sync_target.RemoteSyncTarget(self.getURL())
        self.assertIs(None, remote_target._client)
        remote_target._ensure_connection()
        self.assertIsNot(None, remote_target._client)
        cli = remote_target._client
        remote_target._ensure_connection()
        self.assertIs(cli, remote_target._client)


class TestRemoteSyncTargets(tests.TestCaseWithServer):

    scenarios = [
        ('http', {'server_def': http_server_def,
                  'sync_target_class': http_target.HTTPSyncTarget}),
        ('remote', {'server_def': remote_server_def,
                    'sync_target_class': sync_target.RemoteSyncTarget}),
        ]

    def getSyncTarget(self, path=None):
        if self.server is None:
            self.startServer()
        return self.sync_target_class(self.getURL(path))

    def test_parse_url(self):
        remote_target = self.sync_target_class(
                                     '%s://127.0.0.1:12345/' % self.url_scheme)
        self.assertEqual(self.url_scheme, remote_target._url.scheme)
        self.assertEqual('127.0.0.1', remote_target._url.hostname)
        self.assertEqual(12345, remote_target._url.port)
        self.assertEqual('/', remote_target._url.path)

    def test_no_sync_exchange_object(self):
        remote_target = self.getSyncTarget()
        self.assertEqual(None, remote_target.get_sync_exchange())

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


load_tests = tests.load_with_scenarios
