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
from u1db.backends import CommonBackend
from u1db.vectorclock import VectorClockRev


class SQLiteDatabase(CommonBackend):
    """A U1DB implementation that uses SQLite as its persistence layer."""

    def __init__(self, sqlite_file):
        """Create a new sqlite file."""
        self._db_handle = dbapi2.connect(sqlite_file)
        self._real_machine_id = None
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
            c.execute("CREATE TABLE transaction_log ("
                      " db_rev INTEGER PRIMARY KEY AUTOINCREMENT,"
                      " doc_id TEXT)")
            c.execute("CREATE TABLE document ("
                      " doc_id TEXT PRIMARY KEY,"
                      " doc_rev TEXT,"
                      " doc TEXT)"
                      )
            c.execute("CREATE TABLE u1db_config (name TEXT, value TEXT)")
            c.execute("INSERT INTO u1db_config VALUES ('sql_schema', '0')")

    def _set_machine_id(self, machine_id):
        """Force the machine_id to be set."""
        with self._db_handle:
            c = self._db_handle.cursor()
            c.execute("INSERT INTO u1db_config VALUES ('machine_id', ?)",
                      (machine_id,))
        self._real_machine_id = machine_id

    def _get_machine_id(self):
        if self._real_machine_id is not None:
            return self._real_machine_id
        c = self._db_handle.cursor()
        c.execute("SELECT value FROM u1db_config WHERE name = 'machine_id'")
        val = c.fetchone()
        if val is None:
            return None
        self._real_machine_id = val[0]
        return self._real_machine_id

    _machine_id = property(_get_machine_id)

    def _get_db_rev(self):
        c = self._db_handle.cursor()
        c.execute('SELECT max(db_rev) FROM transaction_log')
        val = c.fetchone()[0]
        if val is None:
            return 0
        return val

    def _allocate_doc_id(self):
        db_rev = self._get_db_rev()
        return 'doc-%d' % (db_rev,)

    def get_doc(self, doc_id):
        c = self._db_handle.cursor()
        c.execute("SELECT doc_rev, doc FROM document WHERE doc_id = ?",
                  (doc_id,))
        val = c.fetchone()
        if val is None:
            return None, None, False
        doc_rev, doc = val
        if doc == 'null':
            doc = None
        return doc_rev, doc, False # (doc_id in self._conflicts)

    def put_doc(self, doc_id, old_doc_rev, doc):
        if doc_id is None:
            raise u1db.InvalidDocId()
        old_doc = None
        with self._db_handle:
            c = self._db_handle.cursor()
            c.execute("SELECT doc_rev, doc FROM document WHERE doc_id=?",
                      (doc_id,))
            val = c.fetchone()
            if val is None:
                old_rev = old_doc = None
            else:
                old_rev, old_doc = val
                if old_rev != old_doc_rev:
                    raise u1db.InvalidDocRev()
            new_rev = self._allocate_doc_rev(old_doc_rev)
            self._put_and_update_indexes(doc_id, old_doc, new_rev, doc, c)
        return new_rev

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc, c):
        # for index in self._indexes.itervalues():
        #     if old_doc is not None:
        #         index.remove_json(doc_id, old_doc)
        #     if doc not in (None, 'null'):
        #         index.add_json(doc_id, doc)
        if old_doc:
            c.execute("UPDATE document SET doc_rev=?, doc=? WHERE doc_id = ?",
                      (new_rev, doc, doc_id))
        else:
            c.execute("INSERT INTO document VALUES (?, ?, ?)",
                      (doc_id, new_rev, doc))
        c.execute("INSERT INTO transaction_log(doc_id) VALUES (?)",
                  (doc_id,))

    def whats_changed(self, old_db_rev=0):
        c = self._db_handle.cursor()
        c.execute("SELECT db_rev, doc_id FROM transaction_log"
                  " WHERE db_rev > ?", (old_db_rev,))
        results = c.fetchall()
        cur_db_rev = 0
        doc_ids = set()
        for db_rev, doc_id in results:
            if db_rev > cur_db_rev:
                cur_db_rev = db_rev
            doc_ids.add(doc_id)
        return cur_db_rev, doc_ids
