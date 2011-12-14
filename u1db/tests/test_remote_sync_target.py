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

from wsgiref import simple_server
import cStringIO
#from paste import httpserver

from u1db import (
    Document,
    errors,
    tests,
    )
from u1db.remote import (
    http_app,
    http_target
    )


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
            pass  # suppress
    #rh = httpserver.WSGIHandler
    return make_server, req_handler, "shutdown", "http"


class TestRemoteSyncTargets(tests.TestCaseWithServer):

    scenarios = [
        ('http', {'server_def': http_server_def,
                  'sync_target_class': http_target.HTTPSyncTarget}),
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
        def receive_doc(doc):
            other_docs.append((doc.doc_id, doc.rev, doc.content))
        new_gen = remote_target.sync_exchange(
                [(Document('doc-here', 'replica:1', '{"value": "here"}'), 10)],
                'replica', last_known_generation=0,
                return_doc_cb=receive_doc)
        self.assertEqual(1, new_gen)
        self.assertGetDoc(db, 'doc-here', 'replica:1', '{"value": "here"}',
                          False)

    def test_sync_exchange_send_failure_and_retry_scenario(self):
        self.startServer()
        def blackhole_getstderr(inst):
            return cStringIO.StringIO()
        self.patch(self.server.RequestHandlerClass, 'get_stderr',
                   blackhole_getstderr)
        db = self.request_state._create_database('test')
        _put_doc_if_newer = db.put_doc_if_newer
        trigger_ids = ['doc-here2']
        def bomb_put_doc_if_newer(doc):
            if doc.doc_id in trigger_ids:
                raise Exception
            return _put_doc_if_newer(doc)
        self.patch(db, 'put_doc_if_newer', bomb_put_doc_if_newer)
        remote_target = self.getSyncTarget('test')
        other_changes = []
        def receive_doc(doc, gen):
            other_changes.append((doc.doc_id, doc.rev, doc.content, gen))
        self.assertRaises(errors.HTTPError, remote_target.sync_exchange,
                [(Document('doc-here', 'replica:1', '{"value": "here"}'), 10),
                 (Document('doc-here2', 'replica:1', '{"value": "here2"}'), 11)
                 ], 'replica', last_known_generation=0,
                return_doc_cb=receive_doc)
        self.assertGetDoc(db, 'doc-here', 'replica:1', '{"value": "here"}',
                          False)
        self.assertEqual(10, db.get_sync_generation('replica'))
        self.assertEqual([], other_changes)
        # retry
        trigger_ids = []
        new_gen = remote_target.sync_exchange(
                [(Document('doc-here2', 'replica:1', '{"value": "here2"}'), 11)
                 ], 'replica', last_known_generation=0,
                return_doc_cb=receive_doc)
        self.assertGetDoc(db, 'doc-here2', 'replica:1', '{"value": "here2"}',
                          False)
        self.assertEqual(11, db.get_sync_generation('replica'))
        self.assertEqual(2, new_gen)
        # bounced back to us
        self.assertEqual([('doc-here', 'replica:1', '{"value": "here"}', 1)],
                         other_changes)

    def test_sync_exchange_receive(self):
        self.startServer()
        db = self.request_state._create_database('test')
        doc = db.create_doc('{"value": "there"}')
        remote_target = self.getSyncTarget('test')
        other_changes = []
        def receive_doc(doc, gen):
            other_changes.append((doc.doc_id, doc.rev, doc.content, gen))
        new_gen = remote_target.sync_exchange(
                        [], 'replica', last_known_generation=0,
                        return_doc_cb=receive_doc)
        self.assertEqual(1, new_gen)
        self.assertEqual([(doc.doc_id, doc.rev, '{"value": "there"}', 1)],
                         other_changes)


load_tests = tests.load_with_scenarios
