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

"""Tests for server state object."""

import os

from u1db import (
    __version__ as _u1db_version,
    tests,
    )
from u1db.remote import (
    server_state,
    )
from u1db.backends import sqlite_backend


class TestServerState(tests.TestCase):

    def setUp(self):
        super(TestServerState, self).setUp()
        self.state = server_state.ServerState()

    def test_set_workingdir(self):
        tempdir = self.createTempDir()
        self.state.set_workingdir(tempdir)
        self.assertTrue(self.state._relpath('path').startswith(tempdir))

    def test_open_database(self):
        tempdir = self.createTempDir()
        self.state.set_workingdir(tempdir)
        path = tempdir + '/test.db'
        self.assertFalse(os.path.exists(path))
        # Create the db, but don't do anything with it
        sqlite_backend.SQLitePartialExpandDatabase(path)
        db = self.state.open_database('test.db')
        self.assertIsInstance(db, sqlite_backend.SQLitePartialExpandDatabase)
