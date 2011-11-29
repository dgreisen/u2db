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

import paste.fixture
import simplejson
import StringIO

from u1db import (
    __version__ as _u1db_version,
    errors,
    tests,
    )

from u1db.remote import (
    http_app,
    http_errors,
    )


class TestFencedReader(tests.TestCase):

    def test_init(self):
        reader = http_app._FencedReader(StringIO.StringIO(""), 25)
        self.assertEqual(25, reader.remaining)

    def test_read_chunk(self):
        inp = StringIO.StringIO("abcdef")
        reader = http_app._FencedReader(inp, 5)
        data = reader.read_chunk(2)
        self.assertEqual("ab", data)
        self.assertEqual(2, inp.tell())
        self.assertEqual(3, reader.remaining)

    def test_read_chunk_remaining(self):
        inp = StringIO.StringIO("abcdef")
        reader = http_app._FencedReader(inp, 4)
        data = reader.read_chunk(9999)
        self.assertEqual("abcd", data)
        self.assertEqual(4, inp.tell())
        self.assertEqual(0, reader.remaining)

    def test_read_chunk_nothing_left(self):
        inp = StringIO.StringIO("abc")
        reader = http_app._FencedReader(inp, 2)
        reader.read_chunk(2)
        self.assertEqual(2, inp.tell())
        self.assertEqual(0, reader.remaining)
        data = reader.read_chunk(2)
        self.assertEqual("", data)
        self.assertEqual(2, inp.tell())
        self.assertEqual(0, reader.remaining)

    def test_read_chunk_kept(self):
        inp = StringIO.StringIO("abcde")
        reader = http_app._FencedReader(inp, 4)
        reader._kept = "xyz"
        data = reader.read_chunk(2) # atmost ignored
        self.assertEqual("xyz", data)
        self.assertEqual(0, inp.tell())
        self.assertEqual(4, reader.remaining)

    def test_getline(self):
        inp = StringIO.StringIO("abc\r\nde")
        reader = http_app._FencedReader(inp, 6)
        reader.MAXCHUNK = 6
        line = reader.getline()
        self.assertEqual("abc\r\n", line)
        self.assertEqual("d", reader._kept)

    def test_getline_exact(self):
        inp = StringIO.StringIO("abcd\r\nef")
        reader = http_app._FencedReader(inp, 6)
        reader.MAXCHUNK = 6
        line = reader.getline()
        self.assertEqual("abcd\r\n", line)
        self.assertIs(None, reader._kept)

    def test_getline_no_newline(self):
        inp = StringIO.StringIO("abcd")
        reader = http_app._FencedReader(inp, 4)
        reader.MAXCHUNK = 6
        line = reader.getline()
        self.assertEqual("abcd", line)

    def test_getline_many_chunks(self):
        inp = StringIO.StringIO("abcde\r\nf")
        reader = http_app._FencedReader(inp, 8)
        reader.MAXCHUNK = 4
        line = reader.getline()
        self.assertEqual("abcde\r\n", line)
        self.assertEqual("f", reader._kept)

    def test_getline_empty(self):
        inp = StringIO.StringIO("")
        reader = http_app._FencedReader(inp, 0)
        reader.MAXCHUNK = 4
        line = reader.getline()
        self.assertEqual("", line)
        line = reader.getline()
        self.assertEqual("", line)

    def test_getline_just_newline(self):
        inp = StringIO.StringIO("\r\n")
        reader = http_app._FencedReader(inp, 2)
        reader.MAXCHUNK = 4
        line = reader.getline()
        self.assertEqual("\r\n", line)
        line = reader.getline()
        self.assertEqual("", line)


class TestHTTPMethodDecorator(tests.TestCase):

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

    def test_args_conversion_with_default(self):
        @http_app.http_method(b=str)
        def f(self, a, b=None):
            return self, a, b
        res = f("self", {"a": "x"}, None)
        self.assertEqual(("self", "x", None), res)

    def test_args_content(self):
        @http_app.http_method()
        def f(self, a, content):
            return a, content
        res = f(self, {"a": "x"}, "CONTENT")
        self.assertEqual(("x", "CONTENT"), res)

    def test_args_content_as_args(self):
        @http_app.http_method(b=int, content_as_args=True)
        def f(self, a, b):
            return self, a, b
        res = f("self", {"a": "x"}, '{"b": "2"}')
        self.assertEqual(("self", "x", 2), res)

        self.assertRaises(http_app.BadRequest, f, "self", {}, 'not-json')

    def test_args_content_no_query(self):
        @http_app.http_method(no_query=True,
                              content_as_args=True)
        def f(self, a='a', b='b'):
            return a, b
        res = f("self", {}, '{"b": "y"}')
        self.assertEqual(('a', 'y'), res)

        self.assertRaises(http_app.BadRequest, f, "self", {'a': 'x'},
                          '{"b": "y"}')

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

    @http_app.http_method(content_as_args=True)
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

class TestHTTPInvocationByMethodWithBody(tests.TestCase):

    def test_get(self):
        resource = TestResource()
        environ = {'QUERY_STRING': 'a=1&b=2', 'REQUEST_METHOD': 'GET'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        res = invoke()
        self.assertEqual('Get', res)
        self.assertEqual({'a': '1', 'b': '2'}, resource.args)

    def test_put_json(self):
        resource = TestResource()
        body = '{"body": true}'
        environ = {'QUERY_STRING': 'a=1', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO(body),
                   'CONTENT_LENGTH': str(len(body)),
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        res = invoke()
        self.assertEqual('Put', res)
        self.assertEqual({'a': '1'}, resource.args)
        self.assertEqual('{"body": true}', resource.content)

    def test_put_multi_json(self):
        resource = TestResource()
        body = (
            '{"b": 2}\r\n'       # args
            '{"entry": "x"}\r\n' # stream entry
            '{"entry": "y"}\r\n' # stream entry
            )
        environ = {'QUERY_STRING': 'a=1', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO(body),
                   'CONTENT_LENGTH': str(len(body)),
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
                   'CONTENT_LENGTH': '2',
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_content_type(self):
        resource = TestResource()
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('{}'),
                   'CONTENT_LENGTH': '2',
                   'CONTENT_TYPE': 'text/plain'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_no_content_length(self):
        resource = TestResource()
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('a'),
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_invalid_content_length(self):
        resource = TestResource()
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('abc'),
                   'CONTENT_LENGTH': '1unk',
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_empty_body(self):
        resource = TestResource()
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO(''),
                   'CONTENT_LENGTH': '0',
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(resource, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_method_get_like(self):
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'DELETE'}
        invoke = http_app.HTTPInvocationByMethodWithBody(None, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_method_put_like(self):
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'PUT',
                   'wsgi.input': StringIO.StringIO('{}'),
                   'CONTENT_LENGTH': '2',
                   'CONTENT_TYPE': 'application/json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(None, environ)
        self.assertRaises(http_app.BadRequest, invoke)

    def test_bad_request_unsupported_method_put_like_multi_json(self):
        body = '{}\r\n{}\r\n'
        environ = {'QUERY_STRING': '', 'REQUEST_METHOD': 'POST',
                   'wsgi.input': StringIO.StringIO(body),
                   'CONTENT_LENGTH': str(len(body)),
                   'CONTENT_TYPE': 'application/x-u1db-multi-json'}
        invoke = http_app.HTTPInvocationByMethodWithBody(None, environ)
        self.assertRaises(http_app.BadRequest, invoke)


class TestHTTPResponder(tests.TestCase):

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
        self.assertEqual([], responder.content)

    def test_send_response_status_fail(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.send_response(400)
        self.assertEqual('400 Bad Request', self.status)
        self.assertEqual({'content-type': 'application/json',
                          'cache-control': 'no-cache'}, self.headers)
        self.assertEqual([], self.response_body)
        self.assertEqual([], responder.content)

    def test_send_response_content_w_headers(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.send_response_content('foo', headers={'x-a': '1'})
        self.assertEqual('200 OK', self.status)
        self.assertEqual({'content-type': 'application/json',
                          'cache-control': 'no-cache',
                          'x-a': '1', 'content-length': '3'}, self.headers)
        self.assertEqual([], self.response_body)
        self.assertEqual(['foo'], responder.content)

    def test_start_finish_response_status_fail(self):
        responder = http_app.HTTPResponder(self.start_response)
        responder.start_response(404, error='not found')
        responder.finish_response()
        self.assertEqual('404 Not Found', self.status)
        self.assertEqual({'content-type': 'application/json',
                          'cache-control': 'no-cache'}, self.headers)
        self.assertEqual(['{"error": "not found"}\r\n'], self.response_body)
        self.assertEqual([], responder.content)

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
        self.assertEqual([], responder.content)

class TestHTTPApp(tests.TestCase):

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

    def test_version(self):
        resp = self.app.get('/')
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({"version": _u1db_version},
                         simplejson.loads(resp.body))

    def test_put_doc_create(self):
        resp = self.app.put('/db0/doc/doc1', params='{"x": 1}',
                            headers={'content-type': 'application/json'})
        doc = self.db0.get_doc('doc1')
        self.assertEqual(201, resp.status) # created
        self.assertEqual('{"x": 1}', doc.content)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({'rev': doc.rev}, simplejson.loads(resp.body))

    def test_put_doc(self):
        doc = self.db0.create_doc('{"x": 1}', doc_id='doc1')
        resp = self.app.put('/db0/doc/doc1?old_rev=%s' % doc.rev,
                            params='{"x": 2}',
                            headers={'content-type': 'application/json'})
        doc = self.db0.get_doc('doc1')
        self.assertEqual(200, resp.status)
        self.assertEqual('{"x": 2}', doc.content)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({'rev': doc.rev}, simplejson.loads(resp.body))

    def test_delete_doc(self):
        doc = self.db0.create_doc('{"x": 1}', doc_id='doc1')
        resp = self.app.delete('/db0/doc/doc1?old_rev=%s' % doc.rev)
        doc = self.db0.get_doc('doc1')
        self.assertEqual(None, doc.content)
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({'rev': doc.rev}, simplejson.loads(resp.body))

    def test_get_doc(self):
        doc = self.db0.create_doc('{"x": 1}', doc_id='doc1')
        resp = self.app.get('/db0/doc/%s' % doc.doc_id)
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual('{"x": 1}', resp.body)
        self.assertEqual(doc.rev, resp.header('x-u1db-rev'))
        self.assertEqual('false', resp.header('x-u1db-has-conflicts'))

    def test_get_doc_non_existing(self):
        resp = self.app.get('/db0/doc/not-there', expect_errors=True)
        self.assertEqual(404, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({"error": "document does not exist"},
                         simplejson.loads(resp.body))
        self.assertEqual('', resp.header('x-u1db-rev'))
        self.assertEqual('false', resp.header('x-u1db-has-conflicts'))

    def test_get_doc_deleted(self):
        doc = self.db0.create_doc('{"x": 1}', doc_id='doc1')
        self.db0.delete_doc(doc)
        resp = self.app.get('/db0/doc/doc1', expect_errors=True)
        self.assertEqual(404, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({"error": errors.DOCUMENT_DELETED},
                         simplejson.loads(resp.body))
        self.assertEqual(doc.rev, resp.header('x-u1db-rev'))
        self.assertEqual('false', resp.header('x-u1db-has-conflicts'))

    def test_get_sync_info(self):
        self.db0.set_sync_generation('other-id', 1)
        resp = self.app.get('/db0/sync-from/other-id')
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual(dict(this_replica_uid='db0',
                              this_replica_generation=0,
                              other_replica_uid='other-id',
                              other_replica_generation=1),
                              simplejson.loads(resp.body))

    def test_record_sync_info(self):
        resp = self.app.put('/db0/sync-from/other-id',
                            params='{"generation": 2}',
                            headers={'content-type': 'application/json'})
        self.assertEqual(200, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({'ok': True}, simplejson.loads(resp.body))
        self.assertEqual(self.db0.get_sync_generation('other-id'), 2)

    def test_sync_exchange_send(self):
        entry = {'id': 'doc-here', 'rev': 'replica:1', 'doc':
                 '{"value": "here"}'}
        args = dict(from_replica_generation=10, last_known_generation=0)
        body = ("%s\r\n" % simplejson.dumps(args) +
                "%s\r\n" % simplejson.dumps(entry))
        resp = self.app.post('/db0/sync-from/replica',
                            params=body,
                            headers={'content-type':
                                     'application/x-u1db-multi-json'})
        self.assertEqual(200, resp.status)
        self.assertEqual('application/x-u1db-multi-json',
                         resp.header('content-type'))
        self.assertEqual({'new_generation': 1}, simplejson.loads(resp.body))
        self.assertGetDoc(self.db0, 'doc-here', 'replica:1',
                          '{"value": "here"}', False)

    def test_sync_exchange_receive(self):
        doc = self.db0.create_doc('{"value": "there"}')
        args = dict(from_replica_generation=10, last_known_generation=0)
        body = "%s\r\n" % simplejson.dumps(args)
        resp = self.app.post('/db0/sync-from/replica',
                            params=body,
                            headers={'content-type':
                                     'application/x-u1db-multi-json'})
        self.assertEqual(200, resp.status)
        self.assertEqual('application/x-u1db-multi-json',
                         resp.header('content-type'))
        parts = resp.body.splitlines()
        self.assertEqual(2, len(parts))
        self.assertEqual({'new_generation': 1}, simplejson.loads(parts[0]))
        self.assertEqual({'doc': '{"value": "there"}',
                          'rev': doc.rev, 'id': doc.doc_id},
                         simplejson.loads(parts[1]))


class TestHTTPErrors(tests.TestCase):

    def test_wire_description_to_status(self):
        self.assertNotIn("error", http_errors.wire_description_to_status)


class TestHTTPAppErrorHandling(tests.TestCase):

    def setUp(self):
        super(TestHTTPAppErrorHandling, self).setUp()
        self.exc = None
        self.state = tests.ServerStateForTests()
        class ErroringResource(object):

            def post(_, args, content):
                raise self.exc

        def lookup_resource(environ, responder):
            return ErroringResource()

        application = http_app.HTTPApp(self.state)
        application._lookup_resource = lookup_resource
        self.app = paste.fixture.TestApp(application)

    def test_RevisionConflict_etc(self):
        self.exc = errors.RevisionConflict()
        resp = self.app.post('/req', params='{}',
                             headers={'content-type': 'application/json'},
                             expect_errors=True)
        self.assertEqual(409, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({"error": "revision conflict"},
                         simplejson.loads(resp.body))

    def test_generic_u1db_errors(self):
        self.exc = errors.U1DBError()
        resp = self.app.post('/req', params='{}',
                             headers={'content-type': 'application/json'},
                             expect_errors=True)
        self.assertEqual(500, resp.status)
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual({"error": "error"},
                         simplejson.loads(resp.body))
