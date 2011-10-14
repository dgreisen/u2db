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

"""A U1DB implementation that uses SQLite as its persistence layer."""

import simplejson
from sqlite3 import dbapi2

import u1db
from u1db.backends import CommonBackend
from u1db import compat


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
            self._extra_schema_init(c)

    def _extra_schema_init(self, c):
        """Add any extra fields, etc to the basic table definitions."""

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
            old_rev, old_doc = self._get_doc(doc_id)
            if old_rev is not None:
                if old_rev != old_doc_rev:
                    raise u1db.InvalidDocRev()
            new_rev = self._allocate_doc_rev(old_doc_rev)
            self._put_and_update_indexes(doc_id, old_doc, new_rev, doc)
        return new_rev

    def _expand_to_fields(self, doc_id, base_field, raw_doc):
        """Convert a dict representation into named fields.

        So something like: {'key1': 'val1', 'key2': 'val2'}
        gets converted into: [(doc_id, 'key1', 'val1', 0)
                              (doc_id, 'key2', 'val2', 0)]
        :param doc_id: Just added to every record.
        :param base_field: if set, these are nested keys, so each field should
            be appropriately prefixed.
        :param raw_doc: The python dictionary.
        """
        # TODO: Handle lists
        values = []
        for field_name, value in raw_doc.iteritems():
            if base_field:
                full_name = base_field + '.' + field_name
            else:
                full_name = field_name
            if value is None or isinstance(value, (int, float, basestring)):
                values.append((doc_id, full_name, value, len(values)))
            else:
                subvalues = self._expand_to_fields(doc_id, full_name, value)
                for _, subfield_name, val, _ in subvalues:
                    values.append((doc_id, subfield_name, val, len(values)))
        return values

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        """Actually insert a document into the database.

        This both updates the existing documents content, and any indexes that
        refer to this document.
        """
        raise NotImplementedError(self._put_and_update_indexes)

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
            old_doc_rev, old_doc = self._get_doc(doc_id)
            if old_doc_rev is None:
                raise KeyError
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
            statement = ("SELECT d.doc_id, d.doc_rev, d.doc FROM document d, "
                         + ', '.join(tables) + " WHERE " + ' AND '.join(where))
            try:
                c.execute(statement, tuple(args))
            except dbapi2.OperationalError, e:
                raise dbapi2.OperationalError(str(e) +
                    '\nstatement: %s\nargs: %s\n' % (statement, args))
            res = c.fetchall()
            result.extend(res)
        return result

    def delete_index(self, index_name):
        with self._db_handle:
            c = self._db_handle.cursor()
            c.execute("DELETE FROM index_definitions WHERE name = ?",
                      (index_name,))


class SQLiteExpandedDatabase(SQLiteDatabase):
    """An SQLite Backend that expands documents into a document_field table.

    It stores the raw document text in document.doc, but also puts the
    individual fields into document_fields.
    """

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        c = self._db_handle.cursor()
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
        values = self._expand_to_fields(doc_id, None, raw_doc)
        # Strip off the 'offset' column.
        values = [x[:3] for x in values]
        c.executemany("INSERT INTO document_fields VALUES (?, ?, ?)",
                      values)
        c.execute("INSERT INTO transaction_log(doc_id) VALUES (?)",
                  (doc_id,))


class SQLitePartialExpandDatabase(SQLiteDatabase):
    """Similar to SQLiteExpandedDatabase, but only indexed fields are expanded.
    """

    def _get_indexed_fields(self):
        """Determine what fields are indexed."""
        c = self._db_handle.cursor()
        c.execute("SELECT field FROM index_definitions")
        return set([x[0] for x in c.fetchall()])

    def _evaluate_index(self, raw_doc, field):
        val = raw_doc
        for subfield in field.split('.'):
            if val is None:
                return None
            val = val.get(subfield, None)
        return val

    def _update_indexes(self, doc_id, raw_doc, fields, db_cursor):
        values = []
        for field_name in fields:
            idx_value = self._evaluate_index(raw_doc, field_name)
            if idx_value is not None:
                values.append((doc_id, field_name, idx_value))
        if values:
            db_cursor.executemany(
                "INSERT INTO document_fields VALUES (?, ?, ?)", values)

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        c = self._db_handle.cursor()
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
        indexed_fields = self._get_indexed_fields()
        if indexed_fields:
            # It is expected that len(indexed_fields) is shorter than
            # len(raw_doc)
            # TODO: Handle nested indexed fields.
            self._update_indexes(doc_id, raw_doc, indexed_fields, c)
        c.execute("INSERT INTO transaction_log(doc_id) VALUES (?)",
                  (doc_id,))

    def create_index(self, index_name, index_expression):
        with self._db_handle:
            c = self._db_handle.cursor()
            cur_fields = self._get_indexed_fields()
            definition = [(index_name, idx, field)
                          for idx, field in enumerate(index_expression)]
            c.executemany("INSERT INTO index_definitions VALUES (?, ?, ?)",
                          definition)
            new_fields = set([f for f in index_expression
                              if f not in cur_fields])
            if new_fields:
                self._update_all_indexes(new_fields)

    def _iter_all_docs(self):
        c = self._db_handle.cursor()
        c.execute("SELECT doc_id, doc FROM document")
        while True:
            next_rows = c.fetchmany()
            if not next_rows:
                break
            for row in next_rows:
                yield row

    def _update_all_indexes(self, new_fields):
        for doc_id, doc in self._iter_all_docs():
            raw_doc = simplejson.loads(doc)
            c = self._db_handle.cursor()
            self._update_indexes(doc_id, raw_doc, new_fields, c)


class SQLiteOnlyExpandedDatabase(SQLiteDatabase):
    """Documents are only stored by their fields.

    Rather than storing the raw content as text, we split it into fields and
    store it in an indexable table.
    """

    def _extra_schema_init(self, c):
        c.execute("ALTER TABLE document_fields ADD COLUMN offset INT")

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        c = self._db_handle.cursor()
        if doc:
            raw_doc = simplejson.loads(doc,
                object_pairs_hook=compat.OrderedDict)
            doc_content = None
        else:
            raw_doc = {}
            doc_content = '<deleted>'
        if old_doc:
            c.execute("UPDATE document SET doc_rev=?, doc=?"
                      " WHERE doc_id = ?", (new_rev, doc_content, doc_id))
            c.execute("DELETE FROM document_fields WHERE doc_id = ?",
                      (doc_id,))
        else:
            c.execute("INSERT INTO document VALUES (?, ?, ?)",
                      (doc_id, new_rev, doc_content))
        values = self._expand_to_fields(doc_id, None, raw_doc)
        c.executemany("INSERT INTO document_fields VALUES (?, ?, ?, ?)",
                      values)
        c.execute("INSERT INTO transaction_log(doc_id) VALUES (?)",
                  (doc_id,))

    def _get_doc(self, doc_id):
        """Get just the document content, without fancy handling."""
        c = self._db_handle.cursor()
        c.execute("SELECT doc_rev, doc FROM document WHERE doc_id = ?",
                  (doc_id,))
        val = c.fetchone()
        if val is None:
            return None, None
        # TODO: There is a race condition here, where we select the document
        #       revision info before we select the actual content fields.
        #       We probably need a transaction (readonly) to ensure
        #       consistency.
        doc_rev, doc_content = val
        if doc_content == '<deleted>':
            return doc_rev, None
        c.execute("SELECT field_name, value FROM document_fields"
                  " WHERE doc_id = ? ORDER BY offset", (doc_id,))
        # TODO: What about nested docs?
        raw_doc = compat.OrderedDict()
        for field, value in c.fetchall():
            if '.' in field: # A nested document
                split = field.split('.')
                cur = raw_doc
                for subfield in split[:-1]:
                    if subfield not in cur:
                        cur[subfield] = {}
                    cur = cur[subfield]
                cur[split[-1]] = value
            else:
                raw_doc[field] = value
        doc = simplejson.dumps(raw_doc)
        return doc_rev, doc

    def get_from_index(self, index_name, key_values):
        # The base implementation does all the complex index joining. But it
        # doesn't manage to extract the actual document content correctly.
        # To do that, we add a loop around self._get_doc
        base = super(SQLiteOnlyExpandedDatabase, self).get_from_index(
            index_name, key_values)
        result = [(doc_id, doc_rev, self._get_doc(doc_id)[1])
                  for doc_id, doc_rev, _ in base]
        return result