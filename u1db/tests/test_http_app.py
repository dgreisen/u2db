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


class TestHTTPMethodDecorator(testtools.TestCase):

    def test_args(self):
        @http_app.http_method()
        def f(self, a, b):
            return self, a, b
        res = f("self", {"a": "x", "b": "y"}, None)
        self.assertEqual(("self", "x", "y"), res)

    def test_args_missing(self):
        @http_app.http_method()
        def f(self, a, b):
            return a, b
        self.assertRaises(http_app.BadRequest, f, "self", {"a": "x"}, None)

    def test_args_unexpected(self):
        @http_app.http_method()
        def f(self, a):
            return a
        self.assertRaises(http_app.BadRequest, f, "self",
                                                  {"a": "x", "c": "z"}, None)

    def test_args_default(self):
        @http_app.http_method()
        def f(self, a, b="z"):
            return a, b
        res = f("self", {"a": "x"}, None)
        self.assertEqual(("x", "z"), res)

    def test_args_conversion(self):
        @http_app.http_method(b=int)
        def f(self, a, b):
            return self, a, b
        res = f("self", {"a": "x", "b": "2"}, None)
        self.assertEqual(("self", "x", 2), res)

        self.assertRaises(http_app.BadRequest, f, "self",
                                                  {"a": "x", "b": "foo"}, None)

    def test_args_content(self):
        @http_app.http_method()
        def f(self, a, content):
            return a, content
        res = f(self, {"a": "x"}, "CONTENT")
        self.assertEqual(("x", "CONTENT"), res)

    def test_args_content_unserialized_as_args(self):
        @http_app.http_method(b=int, content_unserialized_as_args=True)
        def f(self, a, b):
            return self, a, b
        res = f("self", {"a": "x"}, '{"b": "2"}')
        self.assertEqual(("self", "x", 2), res)

        self.assertRaises(http_app.BadRequest, f, "self", {}, 'not-json')

    def test_args_content_no_query(self):
        @http_app.http_method(no_query=True,
                              content_unserialized_as_args=True)
        def f(self, b):
            return b
        res = f("self", {}, '{"b": 2}')
        self.assertEqual(2, res)

        self.assertRaises(http_app.BadRequest, f, "self", {'a': 'x'},
                          '{"b": 2}')

class TestResource(object):

    @http_app.http_method()
    def get(self, a, b):
        self.args = dict(a=a, b=b)
        return 'Get'

    @http_app.http_method()
    def put(self, a, content):
        self.args = dict(a=a)
        self.content = content
        return 'Put'

    @http_app.http_method(content_unserialized_as_args=True)
    def put_args(self, a, b):
        self.args = dict(a=a, b=b)
        self.order = ['a']
        self.entries = []

    @http_app.http_method()
    def put_stream_entry(self, content):
        self.entries.append(content)
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
        self.assertEqual('{"body": true}', resource.content)

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

    def test_bad_request_decode_failure(self):
        resource = TestResource()
        environ = {'QUERY_STRING': 'a=\xff', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('{}'),
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_content_type(self):
        resource = TestResource()
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('{}'),
                   'CONTENT_TYPE': 'text/plain'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_method_get_like(self):
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'DELETE'}
        invoke = http_app.HTTPInvocationByMethodWithBody(None, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_method_put_like(self):
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('{}'),
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(None, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_method_put_like_multi_json(self):
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'POST',
                   'wsgi.input': StringIO.StringIO('{}\r\n{}\r\n'),
                   'CONTENT_TYPE': 'application/x-u1db-multi-json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(None, environ)
        self.assertRaises(http_app.BadRequest, invoke)


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

    def test_bad_request_broken(self):
        resp = self.app.put('/db0/doc/doc1', params='{"x": 1}',
                            headers={'content-type': 'application/foo'},
                            expect_errors=True)
        self.assertEqual(400, resp.status)

    def test_bad_request_dispatch(self):
        resp = self.app.put('/db0/foo/doc1', params='{"x": 1}',
                            headers={'content-type': 'application/json'},
                            expect_errors=True)
        self.assertEqual(400, resp.status)

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
