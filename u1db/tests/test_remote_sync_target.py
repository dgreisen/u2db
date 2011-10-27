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

import os

from u1db import (
    tests,
    )
from u1db.remote import (
    sync_target,
    )
from u1db.backends import (
    sqlite_backend,
    )


class TestRemoteSyncTarget(tests.TestCaseWithSyncServer):

    def getSyncTarget(self, path=None):
        if self.server is None:
            self.startServer()
        return sync_target.RemoteSyncTarget.connect(self.getURL(path))

    def switchToTempDir(self):
        tempdir = self.createTempDir()
        curdir = os.getcwd()
        os.chdir(tempdir)
        self.addCleanup(os.chdir, curdir)

    def test_connect(self):
        self.startServer()
        url = self.getURL()
        remote_target = sync_target.RemoteSyncTarget.connect(url)
        self.assertEqual(url, remote_target._url.geturl())
        self.assertIs(None, remote_target._conn)

    def test_parse_url(self):
        remote_target = sync_target.RemoteSyncTarget('u1db://127.0.0.1:12345/')
        self.assertEqual('u1db', remote_target._url.scheme)
        self.assertEqual('127.0.0.1', remote_target._url.hostname)
        self.assertEqual(12345, remote_target._url.port)
        self.assertEqual('/', remote_target._url.path)

    def test__ensure_connection(self):
        remote_target = self.getSyncTarget()
        self.assertIs(None, remote_target._conn)
        remote_target._ensure_connection()
        self.assertIsNot(None, remote_target._conn)
        c = remote_target._conn
        remote_target._ensure_connection()
        self.assertIs(c, remote_target._conn)
        self.assertIsNot(None, remote_target._client)

    def test_get_sync_info(self):
        self.switchToTempDir()
        local_db = sqlite_backend.SQLitePartialExpandDatabase('test.sqlite')
        local_db._set_machine_id('test-id')
        local_db.set_sync_generation('other-id', 1)
        # TODO: introduce a real close?
        local_db._close_sqlite_handle()
        del local_db
        remote_target = self.getSyncTarget('test.sqlite')
        self.assertEqual(('test-id', 0, 1),
                         remote_target.get_sync_info('other-id'))
