# Copyright 2011 Canonical Ltd.
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

import os
import socket
import subprocess
import sys

from u1db import (
    __version__ as _u1db_version,
    open as u1db_open,
    tests,
    )
from u1db.remote import http_client
from u1db.tests.commandline import safe_close


class TestU1DBServe(tests.TestCase):

    def setUp(self):
        super(TestU1DBServe, self).setUp()
        self.tmp_dir = self.createTempDir('u1db-serve-test')

    def _get_u1db_serve_path(self):
        from u1db import __path__ as u1db_path
        u1db_parent_dir = os.path.dirname(u1db_path[0])
        return os.path.join(u1db_parent_dir, 'u1db-serve')

    def startU1DBServe(self, args):
        command = [sys.executable, self._get_u1db_serve_path()]
        command.extend(args)
        p = subprocess.Popen(command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(safe_close, p)
        return p

    def test_help(self):
        p = self.startU1DBServe(['--help'])
        stdout, stderr = p.communicate()
        if stderr != '':
            # stderr should normally be empty, but if we are running under
            # python-dbg, it contains the following string
            self.assertRegexpMatches(stderr, r'\[\d+ refs\]')
        self.assertEqual(0, p.returncode)
        self.assertIn('Run the U1DB server', stdout)

    def _get_port(self, p, starts='listening on:'):
        x = p.stdout.readline().strip()
        self.assertTrue(x.startswith(starts))
        return int(x[len(starts):].split(":")[1])

    def _request(self, port, path):
        url = "http://127.0.0.1:%s%s" % (port, path)
        c = http_client.HTTPClientBase(url)
        self.addCleanup(c.close)
        return c._request_json('GET', [])

    def test_bind_to_port(self):
        p = self.startU1DBServe([])
        port = self._get_port(p)
        res, _ = self._request(port, '/')
        self.assertIn('version', res)

    def test_supply_port(self):
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        host, port = s.getsockname()
        s.close()
        p = self.startU1DBServe(['--port', str(port)])
        eff_port = self._get_port(p, 'listening on: 127.0.0.1')
        self.assertEqual(port, eff_port)
        res, _ = self._request(port, '/')
        self.assertIn('version', res)

    def test_bind_to_host(self):
        p = self.startU1DBServe(["--host", "localhost"])
        starts = 'listening on: 127.0.0.1:'
        x = p.stdout.readline()
        self.assertTrue(x.startswith(starts))

    def _landmark_db(self, name):
        db = u1db_open(os.path.join(self.tmp_dir, name), create=True)
        db.close()

    def test_supply_working_dir(self):
        self._landmark_db('landmark.db')
        p = self.startU1DBServe(['--working-dir', self.tmp_dir])
        port = self._get_port(p)
        res, _ = self._request(port, '/landmark.db')
        self.assertEqual({}, res)

    def test_accept_cors_connections(self):
        self._landmark_db('landmark2.db')
        origins = ['http://bar.example', 'http://foo.example']
        p = self.startU1DBServe(['--working-dir', self.tmp_dir,
                                 '--accept-cors-connections',
                                 ','.join(origins)])
        port = self._get_port(p)
        res, headers = self._request(port, '/landmark2.db')
        self.assertEqual({}, res)  # sanity
        self.assertIn('access-control-allow-origin', headers)
        self.assertEqual(' '.join(origins),
                         headers['access-control-allow-origin'])

    def test_list_dbs(self):
        self._landmark_db('a.db')
        self._landmark_db('b')
        with open(os.path.join(self.tmp_dir, 'foo'), 'w'):
            pass
        p = self.startU1DBServe(['--working-dir', self.tmp_dir])
        port = self._get_port(p)
        res, _ = self._request(port, '/')
        self.assertIn('databases', res)
        self.assertEqual(2, res['db_count'])
        self.assertEqual({'a.db': None, 'b': None}, res['databases'])
