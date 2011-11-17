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

import inspect
import os
import simplejson
from wsgiref import simple_server

from u1db import (
    tests,
    )
from u1db.remote import (
    http_database
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
        return http_database.HTTPClientBase(self.getURL('dbase'))

    def test_construct(self):
        self.startServer()
        url = self.getURL()
        db = http_database.HTTPClientBase(url)
        self.assertEqual(url, db._url.geturl())
        self.assertIs(None, db._conn)

    def test_parse_url(self):
        db = http_database.HTTPClientBase(
                                     '%s://127.0.0.1:12345/' % self.url_scheme)
        self.assertEqual(self.url_scheme, db._url.scheme)
        self.assertEqual('127.0.0.1', db._url.hostname)
        self.assertEqual(12345, db._url.port)
        self.assertEqual('/', db._url.path)

    def test__ensure_connection(self):
        db = self.getClient()
        self.assertIs(None, db._conn)
        db._ensure_connection()
        self.assertIsNot(None, db._conn)
        cli = db._conn
        db._ensure_connection()
        self.assertIs(cli, db._conn)

    def test__request(self):
        db = self.getClient()
        res, headers = db._request('PUT', ['echo'], {}, {})
        self.assertEqual({'CONTENT_TYPE': 'application/json',
                          'PATH_INFO': '/dbase/echo',
                          'QUERY_STRING': '',
                          'body': '{}',
                          'REQUEST_METHOD': 'PUT'}, simplejson.loads(res))


        res, headers = db._request('GET', ['doc', 'echo'], {'a': 1})
        self.assertEqual({'PATH_INFO': '/dbase/doc/echo',
                          'QUERY_STRING': 'a=1',
                          'REQUEST_METHOD': 'GET'}, simplejson.loads(res))

        res, headers = db._request('POST', ['echo'], {'b': 2}, 'Body',
                                   'application/x-test')
        self.assertEqual({'CONTENT_TYPE': 'application/x-test',
                          'PATH_INFO': '/dbase/echo',
                          'QUERY_STRING': 'b=2',
                          'body': 'Body',
                          'REQUEST_METHOD': 'POST'}, simplejson.loads(res))

    def test__request_json(self):
        db = self.getClient()
        res, headers = db._request_json('POST', ['echo'], {'b': 2}, {'a': 'x'})
        self.assertEqual('application/json', headers['content-type'])
        self.assertEqual({'CONTENT_TYPE': 'application/json',
                          'PATH_INFO': '/dbase/echo',
                          'QUERY_STRING': 'b=2',
                          'body': '{"a": "x"}',
                          'REQUEST_METHOD': 'POST'}, res)


class TestHTTPDatabaseSimpleOperations(tests.TestCase):

    def setUp(self):
        super(TestHTTPDatabaseSimpleOperations, self).setUp()
        self.db = http_database.HTTPDatabase('dbase')
        self.db._conn = object() # crash if used
        self.got = None
        self.response_val = None
        def _request(method, url_parts, params=None, body=None,
                                                     content_type=None):
            self.got = method, url_parts, params, body, content_type
            return self.response_val
        def _request_json(method, url_parts, params=None, body=None,
                                                          content_type=None):
            self.got = method, url_parts, params, body, content_type
            return self.response_val
        self.db._request = _request
        self.db._request_json = _request_json

    def test__sanity_same_signature(self):
        my_request_sig = inspect.getargspec(self.db._request)
        my_request_sig = (['self'] + my_request_sig[0],) + my_request_sig[1:]
        self.assertEqual(my_request_sig,
                       inspect.getargspec(http_database.HTTPDatabase._request))
        my_request_json_sig = inspect.getargspec(self.db._request_json)
        my_request_json_sig = ((['self'] + my_request_json_sig[0],) +
                               my_request_json_sig[1:])
        self.assertEqual(my_request_json_sig,
                  inspect.getargspec(http_database.HTTPDatabase._request_json))

    def test_put_doc(self):
        self.response_val = {'rev': 'doc-rev'}, {}
        res = self.db.put_doc('doc-id', None, '{"v": 1}')
        self.assertEqual('doc-rev', res)
        self.assertEqual(('PUT', ['doc', 'doc-id'], {},
                          '{"v": 1}', 'application/json'), self.got)

        self.response_val = {'rev': 'doc-rev-2'}, {}
        res = self.db.put_doc('doc-id', 'doc-rev', '{"v": 2}')
        self.assertEqual('doc-rev-2', res)
        self.assertEqual(('PUT', ['doc', 'doc-id'], {'old_rev': 'doc-rev'},
                          '{"v": 2}', 'application/json'), self.got)

    def test_get_doc(self):
        self.response_val = '{"v": 2}', {'x-u1db-rev': 'doc-rev',
                                         'x-u1db-has-conflicts': 'false'}
        doc_rev, doc, has_conflicts = self.db.get_doc('doc-id')
        self.assertEqual('doc-rev', doc_rev)
        self.assertEqual('{"v": 2}', doc)
        self.assertEqual(False, has_conflicts)
        self.assertEqual(('GET', ['doc', 'doc-id'], None, None, None),
                         self.got)
