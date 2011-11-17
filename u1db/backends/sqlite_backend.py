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

from u1db.backends import CommonBackend, CommonSyncTarget
from u1db import compat, errors


class SQLiteDatabase(CommonBackend):
    """A U1DB implementation that uses SQLite as its persistence layer."""

    _sqlite_registry = {}

    def __init__(self, sqlite_file):
        """Create a new sqlite file."""
        self._db_handle = dbapi2.connect(sqlite_file)
        self._real_replica_uid = None
        self._ensure_schema()

    def get_sync_target(self):
        return SQLiteSyncTarget(self)

    @staticmethod
    def open_database(sqlite_file):
        # TODO: We really want a way to indicate that the database must already
        # exist.
        db_handle = dbapi2.connect(sqlite_file)
        c = db_handle.cursor()
        c.execute("SELECT value FROM u1db_config WHERE name = 'index_storage'")
        v = c.fetchone()
        # if v is None:
        #     raise ValueError('No defined index_storage for database %s'
        #                      % (sqlite_file,))
        return SQLiteDatabase._sqlite_registry[v[0]](sqlite_file)

    @staticmethod
    def register_implementation(klass):
        """Register that we implement an SQLiteDatabase.

        The attribute _index_storage_value will be used as the lookup key.
        """
        SQLiteDatabase._sqlite_registry[klass._index_storage_value] = klass

    def _get_sqlite_handle(self):
        """Get access to the underlying sqlite database.

        This should only be used by the test suite, etc, for examining the
        state of the underlying database.
        """
        return self._db_handle

    def _close_sqlite_handle(self):
        """Release access to the underlying sqlite database."""
        self._db_handle.close()

    def close(self):
        self._close_sqlite_handle()

    def _is_initialized(self, c):
        """Check if this database has been initialized."""
        c.execute("PRAGMA case_sensitive_like=ON")
        try:
            c.execute("SELECT value FROM u1db_config"
                      " WHERE name = 'sql_schema'")
        except dbapi2.OperationalError, e:
            # The table does not exist yet
            val = None
        else:
            val = c.fetchone()
        if val is not None:
            return True
        return False

    def _initialize(self, c):
        """Create the schema in the database."""
        with self._db_handle:
            c.execute("CREATE TABLE transaction_log ("
                      " generation INTEGER PRIMARY KEY AUTOINCREMENT,"
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
            # TODO: Should we include doc_id or not? By including it, the
            #       content can be returned directly from the index, and
            #       matched with the documents table, roughly saving 1 btree
            #       lookup per query. It costs us extra data storage.
            c.execute("CREATE INDEX document_fields_field_value_doc_idx"
                      " ON document_fields(field_name, value, doc_id)")
            c.execute("CREATE TABLE sync_log ("
                      " replica_uid TEXT PRIMARY KEY,"
                      " known_generation INTEGER)")
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
            c.execute("INSERT INTO u1db_config VALUES" " ('index_storage', ?)",
                      (self._index_storage_value,))
            self._extra_schema_init(c)

    def _ensure_schema(self):
        """Ensure that the database schema has been created."""
        c = self._db_handle.cursor()
        if self._is_initialized(c):
            return
        self._initialize(c)

    def _extra_schema_init(self, c):
        """Add any extra fields, etc to the basic table definitions."""

    def _set_replica_uid(self, replica_uid):
        """Force the replica_uid to be set."""
        with self._db_handle:
            c = self._db_handle.cursor()
            c.execute("INSERT INTO u1db_config VALUES ('replica_uid', ?)",
                      (replica_uid,))
        self._real_replica_uid = replica_uid

    def _get_replica_uid(self):
        if self._real_replica_uid is not None:
            return self._real_replica_uid
        c = self._db_handle.cursor()
        c.execute("SELECT value FROM u1db_config WHERE name = 'replica_uid'")
        val = c.fetchone()
        if val is None:
            return None
        self._real_replica_uid = val[0]
        return self._real_replica_uid

    _replica_uid = property(_get_replica_uid)

    def _get_generation(self):
        c = self._db_handle.cursor()
        c.execute('SELECT max(generation) FROM transaction_log')
        val = c.fetchone()[0]
        if val is None:
            return 0
        return val

    def _allocate_doc_id(self):
        my_gen = self._get_generation()
        return 'doc-%d' % (my_gen,)

    def _get_transaction_log(self):
        c = self._db_handle.cursor()
        c.execute("SELECT doc_id FROM transaction_log ORDER BY generation")
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
            raise errors.InvalidDocId()
        old_doc = None
        with self._db_handle:
            if self._has_conflicts(doc_id):
                raise errors.ConflictedDoc()
            old_rev, old_doc = self._get_doc(doc_id)
            if old_rev is not None:
                if old_rev != old_doc_rev:
                    raise errors.InvalidDocRev()
            else:
                if old_doc_rev is not None:
                    raise errors.InvalidDocRev()
            new_rev = self._allocate_doc_rev(old_doc_rev)
            self._put_and_update_indexes(doc_id, old_doc, new_rev, doc)
        return new_rev

    def _expand_to_fields(self, doc_id, base_field, raw_doc, save_none):
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
            if value is None and not save_none:
                continue
            if base_field:
                full_name = base_field + '.' + field_name
            else:
                full_name = field_name
            if value is None or isinstance(value, (int, float, basestring)):
                values.append((doc_id, full_name, value, len(values)))
            else:
                subvalues = self._expand_to_fields(doc_id, full_name, value,
                                                   save_none)
                for _, subfield_name, val, _ in subvalues:
                    values.append((doc_id, subfield_name, val, len(values)))
        return values

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        """Actually insert a document into the database.

        This both updates the existing documents content, and any indexes that
        refer to this document.
        """
        raise NotImplementedError(self._put_and_update_indexes)

    def whats_changed(self, old_generation=0):
        c = self._db_handle.cursor()
        c.execute("SELECT generation, doc_id FROM transaction_log"
                  " WHERE generation > ?", (old_generation,))
        results = c.fetchall()
        cur_gen = old_generation
        doc_ids = set()
        for gen, doc_id in results:
            if gen > cur_gen:
                cur_gen = gen
            doc_ids.add(doc_id)
        return cur_gen, doc_ids

    def delete_doc(self, doc_id, doc_rev):
        with self._db_handle:
            old_doc_rev, old_doc = self._get_doc(doc_id)
            if old_doc_rev is None:
                raise KeyError
            if old_doc_rev != doc_rev:
                raise errors.InvalidDocRev()
            if old_doc is None:
                raise KeyError
            if self._has_conflicts(doc_id):
                raise errors.ConflictedDoc()
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

    def get_sync_generation(self, other_replica_uid):
        c = self._db_handle.cursor()
        c.execute("SELECT known_generation FROM sync_log WHERE replica_uid = ?",
                  (other_replica_uid,))
        val = c.fetchone()
        if val is None:
            other_gen = 0
        else:
            other_gen = val[0]
        return other_gen

    def set_sync_generation(self, other_replica_uid, other_generation):
        with self._db_handle:
            c = self._db_handle.cursor()
            my_gen = self._get_generation()
            c.execute("INSERT OR REPLACE INTO sync_log VALUES (?, ?)",
                      (other_replica_uid, other_generation))

    def _compare_and_insert_doc(self, doc_id, doc_rev, doc):
        with self._db_handle:
            return super(SQLiteDatabase, self)._compare_and_insert_doc(
                doc_id, doc_rev, doc)

    def _add_conflict(self, c, doc_id, my_doc_rev, my_doc):
        c.execute("INSERT INTO conflicts VALUES (?, ?, ?)",
                  (doc_id, my_doc_rev, my_doc))

    def force_doc_sync_conflict(self, doc_id, doc_rev, doc):
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

    @staticmethod
    def _transform_glob(value, escape_char='.'):
        """Transform the given glob value into a valid LIKE statement.
        """
        to_escape = [escape_char, '%', '_']
        for esc in to_escape:
            value = value.replace(esc, escape_char + esc)
        assert value[-1] == '*'
        return value[:-1] + '%'

    def get_from_index(self, index_name, key_values):
        definition = self._get_index_definition(index_name)
        # First, build the definition. We join the document_fields table
        # against itself, as many times as the 'width' of our definition.
        # We then do a query for each key_value, one-at-a-time.
        # Note: All of these strings are static, we could cache them, etc.
        tables = ["document_fields d%d" % i for i in range(len(definition))]
        novalue_where = ["d.doc_id = d%d.doc_id"
                         " AND d%d.field_name = ?"
                         % (i, i) for i in range(len(definition))]
        wildcard_where = [novalue_where[i]
                          + (" AND d%d.value NOT NULL" % (i,))
                          for i in range(len(definition))]
        exact_where = [novalue_where[i]
                       + (" AND d%d.value = ?" % (i,))
                       for i in range(len(definition))]
        like_where = [novalue_where[i]
                      + (" AND d%d.value LIKE ? ESCAPE '.'" % (i,))
                      for i in range(len(definition))]
        c = self._db_handle.cursor()
        result = []
        is_wildcard = False
        for key_value in key_values:
            # Merge the lists together, so that:
            # [field1, field2, field3], [val1, val2, val3]
            # Becomes:
            # (field1, val1, field2, val2, field3, val3)
            args = []
            where = []
            if len(key_value) != len(definition):
                raise errors.InvalidValueForIndex()
            for idx, (field, value) in enumerate(zip(definition, key_value)):
                args.append(field)
                if value.endswith('*'):
                    if value == '*':
                        where.append(wildcard_where[idx])
                    else:
                        # This is a glob match
                        if is_wildcard:
                            # We can't have a partial wildcard following
                            # another wildcard
                            raise errors.InvalidValueForIndex()
                        where.append(like_where[idx])
                        args.append(self._transform_glob(value))
                    is_wildcard = True
                else:
                    if is_wildcard:
                        raise errors.InvalidValueForIndex()
                    where.append(exact_where[idx])
                    args.append(value)
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


class SQLiteSyncTarget(CommonSyncTarget):

    def get_sync_info(self, other_replica_uid):
        other_gen = self._db.get_sync_generation(other_replica_uid)
        my_gen = self._db._get_generation()
        return self._db._replica_uid, my_gen, other_gen

    def record_sync_info(self, other_replica_uid, other_replica_generation):
        self._db.set_sync_generation(other_replica_uid,
                                     other_replica_generation)


class SQLitePartialExpandDatabase(SQLiteDatabase):
    """An SQLite Backend that expands documents into a document_field table.

    It stores the original document text in document.doc. For fields that are
    indexed, the data goes into document_fields.
    """

    _index_storage_value = 'expand referenced'

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

SQLiteDatabase.register_implementation(SQLitePartialExpandDatabase)
