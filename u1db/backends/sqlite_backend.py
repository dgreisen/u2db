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

import simplejson
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
            c.execute("CREATE TABLE document_fields ("
                      " doc_id TEXT,"
                      " field_name TEXT,"
                      " value TEXT,"
                      " CONSTRAINT document_fields_pkey"
                      " PRIMARY KEY (doc_id, field_name))")
            # TODO: CREATE_INDEX document_fields(value), or maybe
            #       document_fields(field_name, value) or ...
            c.execute("CREATE TABLE sync_log ("
                      " machine_id TEXT PRIMARY KEY,"
                      " known_db_rev INTEGER)")
            c.execute("CREATE TABLE conflicts ("
                      " doc_id TEXT,"
                      " doc_rev TEXT,"
                      " doc TEXT,"
                  " CONSTRAINT conflicts_pkey PRIMARY KEY (doc_id, doc_rev))")
            c.execute("CREATE TABLE index_definitions ("
                      " name TEXT,"
                      " offset INT,"
                      " field TEXT,"
                      " CONSTRAINT index_definitions_pkey"
                      " PRIMARY KEY (name, offset))")
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

    def _get_transaction_log(self):
        c = self._db_handle.cursor()
        c.execute("SELECT doc_id FROM transaction_log ORDER BY db_rev")
        return [v[0] for v in c.fetchall()]

    def _get_doc(self, doc_id):
        """Get just the document content, without fancy handling."""
        c = self._db_handle.cursor()
        c.execute("SELECT doc_rev, doc FROM document WHERE doc_id = ?",
                  (doc_id,))
        val = c.fetchone()
        if val is None:
            return None, None
        doc_rev, doc = val
        return doc_rev, doc

    def _has_conflicts(self, doc_id):
        c = self._db_handle.cursor()
        c.execute("SELECT 1 FROM conflicts WHERE doc_id = ? LIMIT 1",
                  (doc_id,))
        val = c.fetchone()
        if val is None:
            return False
        else:
            return True

    def get_doc(self, doc_id):
        doc_rev, doc = self._get_doc(doc_id)
        if doc == 'null':
            doc = None
        return doc_rev, doc, self._has_conflicts(doc_id)

    def put_doc(self, doc_id, old_doc_rev, doc):
        if doc_id is None:
            raise u1db.InvalidDocId()
        old_doc = None
        with self._db_handle:
            if self._has_conflicts(doc_id):
                raise u1db.ConflictedDoc()
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
            self._put_and_update_indexes(doc_id, old_doc, new_rev, doc)
        return new_rev

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        c = self._db_handle.cursor()
        # for index in self._indexes.itervalues():
        #     if old_doc is not None:
        #         index.remove_json(doc_id, old_doc)
        #     if doc not in (None, 'null'):
        #         index.add_json(doc_id, doc)
        if doc:
            raw_doc = simplejson.loads(doc)
        else:
            raw_doc = {}
        if old_doc:
            c.execute("UPDATE document SET doc_rev=?, doc=? WHERE doc_id = ?",
                      (new_rev, doc, doc_id))
            c.execute("DELETE FROM document_fields WHERE doc_id = ?",
                      (doc_id,))
        else:
            c.execute("INSERT INTO document VALUES (?, ?, ?)",
                      (doc_id, new_rev, doc))
        values = [(doc_id, field_name, value) for field_name, value in
                  raw_doc.iteritems()]
        c.executemany("INSERT INTO document_fields VALUES (?, ?, ?)",
                      values)
        c.execute("INSERT INTO transaction_log(doc_id) VALUES (?)",
                  (doc_id,))

    def whats_changed(self, old_db_rev=0):
        c = self._db_handle.cursor()
        c.execute("SELECT db_rev, doc_id FROM transaction_log"
                  " WHERE db_rev > ?", (old_db_rev,))
        results = c.fetchall()
        cur_db_rev = old_db_rev
        doc_ids = set()
        for db_rev, doc_id in results:
            if db_rev > cur_db_rev:
                cur_db_rev = db_rev
            doc_ids.add(doc_id)
        return cur_db_rev, doc_ids

    def delete_doc(self, doc_id, doc_rev):
        with self._db_handle:
            c = self._db_handle.cursor()
            c.execute("SELECT doc_rev, doc FROM document WHERE doc_id = ?",
                      (doc_id,))
            val = c.fetchone()
            if val is None:
                raise KeyError
            old_doc_rev, old_doc = val
            if old_doc_rev != doc_rev:
                raise u1db.InvalidDocRev()
            if old_doc is None:
                raise KeyError
            if self._has_conflicts(doc_id):
                raise u1db.ConflictedDoc()
            new_rev = self._allocate_doc_rev(old_doc_rev)
            self._put_and_update_indexes(doc_id, old_doc, new_rev, None)
        return new_rev

    def _get_conflicts(self, doc_id):
        c = self._db_handle.cursor()
        c.execute("SELECT doc_rev, doc FROM conflicts WHERE doc_id = ?",
                  (doc_id,))
        return c.fetchall()

    def get_doc_conflicts(self, doc_id):
        with self._db_handle:
            conflict_docs = self._get_conflicts(doc_id)
            if not conflict_docs:
                return conflict_docs
            this_doc_rev, this_doc = self._get_doc(doc_id)
        return [(this_doc_rev, this_doc)] + conflict_docs

    def _get_sync_info(self, other_machine_id):
        c = self._db_handle.cursor()
        my_db_rev = self._get_db_rev()
        c.execute("SELECT known_db_rev FROM sync_log WHERE machine_id = ?",
                  (other_machine_id,))
        val = c.fetchone()
        if val is None:
            other_db_rev = 0
        else:
            other_db_rev = val[0]

        return self._machine_id, my_db_rev, other_db_rev

    def _record_sync_info(self, machine_id, db_rev):
        with self._db_handle:
            c = self._db_handle.cursor()
            my_db_rev = self._get_db_rev()
            c.execute("INSERT OR REPLACE INTO sync_log VALUES (?, ?)",
                      (machine_id, db_rev))

    def _compare_and_insert_doc(self, doc_id, doc_rev, doc):
        with self._db_handle:
            return super(SQLiteDatabase, self)._compare_and_insert_doc(
                doc_id, doc_rev, doc)

    def _add_conflict(self, c, doc_id, my_doc_rev, my_doc):
        c.execute("INSERT INTO conflicts VALUES (?, ?, ?)",
                  (doc_id, my_doc_rev, my_doc))

    def _put_as_conflict(self, doc_id, doc_rev, doc):
        with self._db_handle:
            my_doc_rev, my_doc = self._get_doc(doc_id)
            c = self._db_handle.cursor()
            self._add_conflict(c, doc_id, my_doc_rev, my_doc)
            self._put_and_update_indexes(doc_id, my_doc, doc_rev, doc)

    def resolve_doc(self, doc_id, doc, conflicted_doc_revs):
        with self._db_handle:
            cur_rev, cur_doc = self._get_doc(doc_id)
            new_rev = self._ensure_maximal_rev(cur_rev, conflicted_doc_revs)
            superseded_revs = set(conflicted_doc_revs)
            cur_conflicts = self._get_conflicts(doc_id)
            c = self._db_handle.cursor()
            if cur_rev in superseded_revs:
                self._put_and_update_indexes(doc_id, cur_doc, new_rev, doc)
            else:
                self._add_conflict(c, doc_id, new_rev, doc)
            deleting = [(doc_id, c_rev) for c_rev in superseded_revs]
            c.executemany("DELETE FROM conflicts"
                          " WHERE doc_id=? AND doc_rev=?", deleting)
            return new_rev, self._has_conflicts(doc_id)

    def create_index(self, index_name, index_expression):
        with self._db_handle:
            c = self._db_handle.cursor()
            definition = [(index_name, idx, field)
                          for idx, field in enumerate(index_expression)]
            c.executemany("INSERT INTO index_definitions VALUES (?, ?, ?)",
                          definition)

    def list_indexes(self):
        """Return the list of indexes and their definitions."""
        c = self._db_handle.cursor()
        # TODO: How do we test the ordering?
        c.execute("SELECT name, field FROM index_definitions"
                  " ORDER BY name, offset")
        definitions = []
        cur_name = None
        for name, field in c.fetchall():
            if cur_name != name:
                definitions.append((name, []))
                cur_name = name
            definitions[-1][-1].append(field)
        return definitions

    def _get_index_definition(self, index_name):
        """Return the stored definition for a given index_name."""
        c = self._db_handle.cursor()
        c.execute("SELECT field FROM index_definitions"
                  " WHERE name = ? ORDER BY offset", (index_name,))
        return [x[0] for x in c.fetchall()]

    def get_from_index(self, index_name, key_values):
        definition = self._get_index_definition(index_name)
        # First, build the definition. We join the document_fields table
        # against itself, as many times as the 'width' of our definition.
        # We then do a query for each key_value, one-at-a-time.
        tables = ["document_fields d%d" % i for i in range(len(definition))]
        where = ["d.doc_id = d%d.doc_id"
                 " AND d%d.field_name = ?"
                 " AND d%d.value = ?"
                 % (i, i, i) for i in range(len(definition))]
        c = self._db_handle.cursor()
        result = []
        for key_value in key_values:
            # Merge the lists together, so that:
            # [field1, field2, field3], [val1, val2, val3]
            # Becomes:
            # (field1, val1, field2, val2, field3, val3)
            args = []
            for field, val in zip(definition, key_value):
                args.append(field)
                args.append(val)
            c.execute("SELECT d.doc_id, d.doc_rev, d.doc FROM document d,"
                      + ', '.join(tables) + " WHERE " + ', '.join(where),
                      tuple(args))
            res = c.fetchone()
            if res is None:
                continue
            result.append(res)
        return result

