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

"""State for servers exposing a set of U1DB databases."""

from u1db import (
    __version__ as _u1db_version,
    )


class ServerState(object):
    """Passed to a Request when it is instantiated.

    This is used to track server-side state, such as working-directory, open
    databases, etc.
    """

    def __init__(self):
        self._workingdir = None

    def set_workingdir(self, path):
        self._workingdir = path

    def _relpath(self, relpath):
        # Note: We don't want to allow absolute paths here, because we
        #       don't want to expose the filesystem. We should also check that
        #       relpath doesn't have '..' in it, etc.
        return self._workingdir + '/' + relpath

    def open_database(self, path):
        """Open a database at the given location."""
        from u1db.backends import sqlite_backend
        full_path = self._relpath(path)
        return sqlite_backend.SQLiteDatabase.open_database(full_path)
