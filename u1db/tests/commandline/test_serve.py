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

import errno
import os
import socket
import subprocess
import sys

from u1db import (
    __version__ as _u1db_version,
    tests,
    )
from u1db.remote import client
from u1db.tests.commandline import safe_close


class TestU1DBServe(tests.TestCase):

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
        self.assertEqual('', stderr)
        self.assertEqual(0, p.returncode)
        self.assertIn('Run the U1DB server', stdout)

    def test_bind_to_port(self):
        p = self.startU1DBServe([])
        starts = 'listening on port:'
        x = p.stdout.readline()
        self.assertTrue(x.startswith(starts))
        port = int(x[len(starts):])
        s = socket.socket()
        s.connect(('127.0.0.1', port))
        self.addCleanup(s.close)
        c = client.Client(s)
        self.assertEqual({'version': _u1db_version},
                         c.call_returning_args('version'))

    def test_supply_port(self):
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        host, port = s.getsockname()
        s.close()
        p = self.startU1DBServe(['--port', str(port)])
        x = p.stdout.readline()
        self.assertEqual('listening on port: %s\n' % (port,), x)
        s = socket.socket()
        s.connect(('127.0.0.1', port))
        self.addCleanup(s.close)
        c = client.Client(s)
        self.assertEqual({'version': _u1db_version},
                         c.call_returning_args('version'))
