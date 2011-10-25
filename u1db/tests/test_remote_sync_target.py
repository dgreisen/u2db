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

"""Tests for the RemoteSyncTarget"""

import threading

from u1db import (
    tests,
    )
from u1db.remote import (
    sync_server,
    sync_target,
    )


class TestCaseWithSyncServer(tests.TestCase):

    def startServer(self):
        self.server = sync_server.TCPSyncServer(
            ('127.0.0.1', 0), sync_server.TCPSyncRequestHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever,
                                              kwargs=dict(poll_interval=0.01))
        self.server_thread.start()
        self.addCleanup(self.server_thread.join)
        self.addCleanup(self.server.force_shutdown)

    def getURL(self):
        host, port = self.server.server_address
        return 'u1db://%s:%s/' % (host, port)


class TestTestCaseWithSyncServer(TestCaseWithSyncServer):

    def test_getURL(self):
        self.startServer()
        url = self.getURL()
        self.assertTrue(url.startswith('u1db://127.0.0.1:'))



class TestRemoteSyncTarget(TestCaseWithSyncServer):

    def test_connect(self):
        self.startServer()
        url = self.getURL()
        remote_target = sync_target.RemoteSyncTarget.connect(url)
        self.assertEqual(url, remote_target._url.geturl())
        self.assertIs(None, remote_target._conn)

    def test__parse_url(self):
        remote_target = sync_target.RemoteSyncTarget('u1db://127.0.0.1:12345/')
        self.assertEqual('u1db', remote_target._url.scheme)
        self.assertEqual('127.0.0.1', remote_target._url.hostname)
        self.assertEqual(12345, remote_target._url.port)
        self.assertEqual('/', remote_target._url.path)
