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

"""Test the WSGI app."""

import testtools
import paste.fixture
import json
import StringIO

from u1db import (
    tests,
    )

from u1db.remote import (
    http_app,
    )


class TestResource(object):

    def get(self, args):
        self.args = args
        return 'Get'

    def put(self, body, args):
        self.body = body
        self.args = args
        return 'Put'

    def put_args(self, args):
        self.args = args
        self.order = ['a']
        self.entries = []

    def put_stream_entry(self, entry):
        self.entries.append(entry)
        self.order.append('s')

    def put_end(self):
        self.order.append('e')
        return "Put/end"

class TestHTTPInvocationByMethodWithBody(testtools.TestCase):

    def test_get(self):
        resource = TestResource()
        environ = {'QUERY_STRING': 'a=1&b=2', 'REQUEST_METHOD': 'GET'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        res = invoke()
        self.assertEqual('Get', res)
        self.assertEqual({'a': '1', 'b': '2'}, resource.args)

    def test_put_json(self):
        resource = TestResource()
        environ = {'QUERY_STRING': 'a=1', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('{"body": true}'),
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        res = invoke()
        self.assertEqual('Put', res)
        self.assertEqual({'a': '1'}, resource.args)
        self.assertEqual('{"body": true}', resource.body)

    def test_put_multi_json(self):
        resource = TestResource()
        environ = {'QUERY_STRING': 'a=1', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO(
                       '{"b": 2}\r\n'       # args
                       '{"entry": "x"}\r\n' # stream entry
                       '{"entry": "y"}\r\n' # stream entry
                       ),
                   'CONTENT_TYPE': 'application/x-u1db-multi-json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        res = invoke()
        self.assertEqual('Put/end', res)
        self.assertEqual({'a': '1', 'b': 2}, resource.args)
        self.assertEqual(['{"entry": "x"}', '{"entry": "y"}'], resource.entries)
        self.assertEqual(['a', 's', 's', 'e'], resource.order)


class TestHTTPResponder(testtools.TestCase):

    def start_response(self, status, headers):
        self.status = status
        self.headers = dict(headers)
        self.response_body = []
        def write(data):
            self.response_body.append(data)
        return write

    def test_send_response(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.send_response(value='success')
        self.assertEqual('200 OK', self.status)
        self.assertEqual({'content-type': 'application/json',
                          'cache-control': 'no-cache'}, self.headers)
        self.assertEqual(['{"value": "success"}\r\n'], self.response_body)

    def test_send_response_status_fail(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.send_response(400)
        self.assertEqual('400 Bad Request', self.status)
        self.assertEqual({'content-type': 'application/json',
                          'cache-control': 'no-cache'}, self.headers)
        self.assertEqual([], self.response_body)

    def test_start_finish_response_status_fail(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.start_response(404, error='not found')
        responder.finish_response()
        self.assertEqual('404 Not Found', self.status)
        self.assertEqual({'content-type': 'application/json',
                          'cache-control': 'no-cache'}, self.headers)
        self.assertEqual(['{"error": "not found"}\r\n'], self.response_body)

    def test_send_stream_entry(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.content_type = "application/x-u1db-multi-json"
        responder.start_response(one=1)
        responder.stream_entry({'entry': True})
        responder.finish_response()
        self.assertEqual('200 OK', self.status)
        self.assertEqual({'content-type': 'application/x-u1db-multi-json',
                          'cache-control': 'no-cache'}, self.headers)
        self.assertEqual(['{"one": 1}\r\n',
                          '{"entry": true}\r\n'], self.response_body)


class TestHTTPApp(testtools.TestCase):

    def setUp(self):
        super(TestHTTPApp, self).setUp()
        self.state = tests.ServerStateForTests()
        application = http_app.HTTPApp(self.state)
        self.app = paste.fixture.TestApp(application)
        self.db0 = self.state._create_database('db0')

    def test_put_doc(self):
        resp = self.app.put('/db0/doc/doc1', params='{"x": 1}',
                            headers={'content-type': 'application/json'})
        doc_rev, doc, _ = self.db0.get_doc('doc1')
        self.assertEqual(200, resp.status)
        self.assertEqual('{"x": 1}', doc)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({'rev': doc_rev}, json.loads(resp.body))

    def test_get_sync_info(self):
        self.db0.set_sync_generation('other-id', 1)
        resp = self.app.get('/db0/sync-from/other-id')
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual(dict(this_replica_uid='db0',
                              this_replica_generation=0,
                              other_replica_uid='other-id',
                              other_replica_generation=1),
                              json.loads(resp.body))

    def test_record_sync_info(self):
        resp = self.app.put('/db0/sync-from/other-id',
                            params='{"generation": 2}',
                            headers={'content-type': 'application/json'})
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({'ok': True}, json.loads(resp.body))
        self.assertEqual(self.db0.get_sync_generation('other-id'), 2)

    def test_sync_exchange_send(self):
        entry = {'id': 'doc-here', 'rev': 'replica:1', 'doc':
                 '{"value": "here"}'}
        args = dict(from_replica_generation=10, last_known_generation=0)
        body = ("%s\r\n" % json.dumps(args) +
                "%s\r\n" % json.dumps(entry))
        resp = self.app.post('/db0/sync-from/replica',
                            params=body,
                            headers={'content-type':
                                     'application/x-u1db-multi-json'})
        self.assertEqual(200, resp.status)
        self.assertEqual('application/x-u1db-multi-json',
                         resp.header('content-type'))
        self.assertEqual({'new_generation': 1}, json.loads(resp.body))
        self.assertEqual(('replica:1', '{"value": "here"}', False),
                         self.db0.get_doc('doc-here'))

    def test_sync_exchange_receive(self):
        doc_id, doc_rev = self.db0.create_doc('{"value": "there"}')
        args = dict(from_replica_generation=10, last_known_generation=0)
        body = "%s\r\n" % json.dumps(args)
        resp = self.app.post('/db0/sync-from/replica',
                            params=body,
                            headers={'content-type':
                                     'application/x-u1db-multi-json'})
        self.assertEqual(200, resp.status)
        self.assertEqual('application/x-u1db-multi-json',
                         resp.header('content-type'))
        parts = resp.body.splitlines()
        self.assertEqual(2, len(parts))
        self.assertEqual({'new_generation': 1}, json.loads(parts[0]))
        self.assertEqual({'doc': '{"value": "there"}',
                          'rev': doc_rev, 'id': doc_id}, json.loads(parts[1]))
