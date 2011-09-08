# Copyright (C) 2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""A U1DB implementation that uses SQLite as its persistence layer."""

from sqlite3 import dbapi2

import u1db
from u1db.vectorclock import VectorClockRev


class SQLiteDatabase(u1db.Database):
    """A U1DB implementation that uses SQLite as its persistence layer."""

    def __init__(self, sqlite_file):
        """Create a new sqlite file."""
        self._db_handle = dbapi2.connect(sqlite_file)
        self._ensure_schema()

    def _get_sqlite_handle(self):
        """Get access to the underlying sqlite database.

        This should only be used by the test suite, etc, for examining the
        state of the underlying database.
        """
        return self._db_handle

    def _close_sqlite_handle(self):
        """Release access to the underlying sqlite database."""
        self._db_handle.close()

    def _ensure_schema(self):
        """Ensure that the database schema has been created."""
        c = self._db_handle.cursor()
        try:
            c.execute("SELECT value FROM u1db_config"
                      " WHERE name = 'sql_schema'")
        except dbapi2.OperationalError, e:
            # The table does not exist yet
            val = None
        else:
            val = c.fetchone()
        if val is not None:
            return
        with self._db_handle:
            c.execute("CREATE TABLE u1db_config (name TEXT, value TEXT)")
            c.execute("INSERT INTO u1db_config VALUES ('sql_schema', '0')")
