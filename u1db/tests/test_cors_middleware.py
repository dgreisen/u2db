# Copyright 2012 Canonical Ltd.
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

"""Test CORS wsgi middleware"""
import paste.fixture

from u1db import tests

from u1db.remote.cors_middleware import CORSMiddleware


class TestCORSMiddleware(tests.TestCase):

    def setUp(self):
        super(TestCORSMiddleware, self).setUp()

    def app(self, accept_cors_connections):

        def base_app(environ, start_response):
            start_response("200 OK", [("content-type", "application/json")])
            return ['{}']

        return paste.fixture.TestApp(CORSMiddleware(base_app,
                                                    accept_cors_connections))

    def _check_cors_headers(self, resp, expect):
        self.assertEqual(expect, resp.header('access-control-allow-origin'))
        self.assertEqual("GET, POST, PUT, DELETE, OPTIONS",
                         resp.header('access-control-allow-methods'))
        self.assertEqual("authorization, content-type, x-requested-with",
                         resp.header('access-control-allow-headers'))

    def test_options(self):
        app = self.app(['*'])
        resp = app._gen_request('OPTIONS', '/')
        self.assertEqual(200, resp.status)
        self._check_cors_headers(resp, '*')
        self.assertEqual('', resp.body)

        app = self.app(['http://bar.example', 'http://foo.example'])
        resp = app._gen_request('OPTIONS', '/db1')
        self.assertEqual(200, resp.status)
        self._check_cors_headers(resp, 'http://bar.example http://foo.example')
        self.assertEqual('', resp.body)

    def test_pass_through(self):
        app = self.app(['*'])
        resp = app.get('/db0')
        self.assertEqual(200, resp.status)
        self._check_cors_headers(resp, '*')
        self.assertEqual('application/json', resp.header('content-type'))
        self.assertEqual('{}', resp.body)

