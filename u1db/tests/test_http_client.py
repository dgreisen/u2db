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

"""Tests for HTTPDatabase"""

import simplejson
from wsgiref import simple_server

from u1db import (
    errors,
    tests,
    )
from u1db.remote import (
    http_client,
    )


class TestHTTPClientBase(tests.TestCaseWithServer):

    def app(self, environ, start_response):
        if environ['PATH_INFO'].endswith('echo'):
            start_response("200 OK", [('Content-Type', 'application/json')])
            ret = {}
            for name in ('REQUEST_METHOD', 'PATH_INFO', 'QUERY_STRING'):
                ret[name] = environ[name]
            if environ['REQUEST_METHOD'] in ('PUT', 'POST'):
                ret['CONTENT_TYPE'] = environ['CONTENT_TYPE']
                content_length = int(environ['CONTENT_LENGTH'])
                ret['body'] = environ['wsgi.input'].read(content_length)
            return [simplejson.dumps(ret)]
        elif environ['PATH_INFO'].endswith('error'):
            content_length = int(environ['CONTENT_LENGTH'])
            error = simplejson.loads(environ['wsgi.input'].read(content_length))
            response = error['response']
            if isinstance(response, basestring):
                start_response(error['status'],
                               [('Content-Type', 'text/plain')])
                return [response]
            else:
                start_response(error['status'],
                               [('Content-Type', 'application/json')])
                return [simplejson.dumps(error['response'])]

    def server_def(self):
        def make_server(host_port, handler, state):
            srv = simple_server.WSGIServer(host_port, handler)
            srv.set_app(self.app)
            return srv
        class req_handler(simple_server.WSGIRequestHandler):
            def log_request(*args):
                pass # suppress
        return make_server, req_handler, "shutdown", "http"

    def getClient(self):
        self.startServer()
        return http_client.HTTPClientBase(self.getURL('dbase'))

    def test_construct(self):
        self.startServer()
        url = self.getURL()
        cli = http_client.HTTPClientBase(url)
        self.assertEqual(url, cli._url.geturl())
        self.assertIs(None, cli._conn)

    def test_parse_url(self):
        cli = http_client.HTTPClientBase(
                                     '%s://127.0.0.1:12345/' % self.url_scheme)
        self.assertEqual(self.url_scheme, cli._url.scheme)
        self.assertEqual('127.0.0.1', cli._url.hostname)
        self.assertEqual(12345, cli._url.port)
        self.assertEqual('/', cli._url.path)

    def test__ensure_connection(self):
        cli = self.getClient()
        self.assertIs(None, cli._conn)
        cli._ensure_connection()
        self.assertIsNot(None, cli._conn)
        conn = cli._conn
        cli._ensure_connection()
        self.assertIs(conn, cli._conn)

    def test_close(self):
        cli = self.getClient()
        cli._ensure_connection()
        cli.close()
        self.assertIs(None, cli._conn)

    def test__request(self):
        cli = self.getClient()
        res, headers = cli._request('PUT', ['echo'], {}, {})
        self.assertEqual({'CONTENT_TYPE': 'application/json',
                          'PATH_INFO': '/dbase/echo',
                          'QUERY_STRING': '',
                          'body': '{}',
                          'REQUEST_METHOD': 'PUT'}, simplejson.loads(res))


        res, headers = cli._request('GET', ['doc', 'echo'], {'a': 1})
        self.assertEqual({'PATH_INFO': '/dbase/doc/echo',
                          'QUERY_STRING': 'a=1',
                          'REQUEST_METHOD': 'GET'}, simplejson.loads(res))

        res, headers = cli._request('GET', ['doc', '%FFFF', 'echo'], {'a': 1})
        self.assertEqual({'PATH_INFO': '/dbase/doc/%FFFF/echo',
                          'QUERY_STRING': 'a=1',
                          'REQUEST_METHOD': 'GET'}, simplejson.loads(res))

        res, headers = cli._request('POST', ['echo'], {'b': 2}, 'Body',
                                   'application/x-test')
        self.assertEqual({'CONTENT_TYPE': 'application/x-test',
                          'PATH_INFO': '/dbase/echo',
                          'QUERY_STRING': 'b=2',
                          'body': 'Body',
                          'REQUEST_METHOD': 'POST'}, simplejson.loads(res))

    def test__request_json(self):
        cli = self.getClient()
        res, headers = cli._request_json('POST', ['echo'], {'b': 2}, {'a': 'x'})
        self.assertEqual('application/json', headers['content-type'])
        self.assertEqual({'CONTENT_TYPE': 'application/json',
                          'PATH_INFO': '/dbase/echo',
                          'QUERY_STRING': 'b=2',
                          'body': '{"a": "x"}',
                          'REQUEST_METHOD': 'POST'}, res)

    def test_unspecified_http_error(self):
        cli = self.getClient()
        self.assertRaises(errors.HTTPError,
                          cli._request_json, 'POST', ['error'], {},
                          {'status': "500 Internal Error",
                           'response': "Crash."})
        try:
            cli._request_json('POST', ['error'], {},
                              {'status': "500 Internal Error",
                               'response': "Fail."})
        except errors.HTTPError, e:
            pass

        self.assertEqual(500, e.status)
        self.assertEqual("Fail.", e.message)
        self.assertTrue("content-type" in e.headers)

    def test_revision_conflict(self):
        cli = self.getClient()
        self.assertRaises(errors.RevisionConflict,
                          cli._request_json, 'POST', ['error'], {},
                          {'status': "409 Conflict",
                           'response': {"error": "revision conflict"}})

    def test_generic_u1db_error(self):
        cli = self.getClient()
        self.assertRaises(errors.U1DBError,
                          cli._request_json, 'POST', ['error'], {},
                          {'status': "400 Bad Request",
                           'response': {"error": "error"}})
        try:
            cli._request_json('POST', ['error'], {},
                              {'status': "400 Bad Request",
                               'response': {"error": "error"}})
        except errors.U1DBError, e:
            pass
        self.assertIs(e.__class__, errors.U1DBError)

    def test_unspecified_bad_request(self):
        cli = self.getClient()
        self.assertRaises(errors.HTTPError,
                          cli._request_json, 'POST', ['error'], {},
                          {'status': "400 Bad Request",
                           'response': "<Bad Request>"})
        try:
            cli._request_json('POST', ['error'], {},
                              {'status': "400 Bad Request",
                               'response': "<Bad Request>"})
        except errors.HTTPError, e:
            pass

        self.assertEqual(400, e.status)
        self.assertEqual("<Bad Request>", e.message)
        self.assertTrue("content-type" in e.headers)
