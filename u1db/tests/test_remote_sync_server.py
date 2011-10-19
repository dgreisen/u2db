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

"""Tests for the remote synchronization server"""

from u1db import (
    errors,
    remote_sync_server,
    tests,
    )
from u1db.backends import inmemory


class TestRemoteSyncServer(tests.TestCase):

    def test_takes_database(self):
        db =  inmemory.InMemoryDatabase('test')
        server = remote_sync_server.RemoteSyncServer(db)


