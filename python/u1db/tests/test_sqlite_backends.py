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

"""Test sqlite backend internals."""

from sqlite3 import dbapi2

from u1db import (
    tests,
    )
from u1db.backends import sqlite_backend, c_wrapper


simple_doc = '{"key": "value"}'


class SQLiteDatabaseTests(object):

    def create_database(self, fname):
        raise NotImplementedError(self.create_database)

    def test_create_database_initializes_schema(self):
        db = self.create_database(':memory:')
        config = dict([(r[0], r[1])
                      for r in db._run_sql("SELECT * FROM u1db_config")])
        self.assertEqual({'sql_schema': '0'}, config)

        # These tables must exist, though we don't care what is in them yet
        db._run_sql("SELECT * FROM transaction_log")
        db._run_sql("SELECT * FROM document")
        db._run_sql("SELECT * FROM document_fields")
        db._run_sql("SELECT * FROM sync_log")
        db._run_sql("SELECT * FROM conflicts")
        db._run_sql("SELECT * FROM index_definitions")

    def test__set_machine_id(self):
        db = self.create_database(':memory:')
        self.assertEqual(None, db._machine_id)
        db._set_machine_id('foo')
        res = db._run_sql("SELECT value FROM u1db_config WHERE name='machine_id'")
        self.assertEqual([('foo',)], res)
        db._close_sqlite_handle()
        self.assertEqual('foo', db._machine_id)
        self.assertEqual('foo', db._machine_id)

    def test__get_db_rev(self):
        db = self.create_database(':memory:')
        db._set_machine_id('foo')
        self.assertEqual(0, db._get_db_rev())

    def test__allocate_doc_id(self):
        db = self.create_database(':memory:')
        self.assertEqual('doc-0', db._allocate_doc_id())

    def test_create_index(self):
        db = self.create_database(':memory:')
        db.create_index('test-idx', ["key"])
        self.assertEqual([('test-idx', ["key"])], db.list_indexes())

    def test_create_index_multiple_fields(self):
        db = self.create_database(':memory:')
        db.create_index('test-idx', ["key", "key2"])
        self.assertEqual([('test-idx', ["key", "key2"])], db.list_indexes())

    def test__get_index_definition(self):
        db = self.create_database(':memory:')
        db.create_index('test-idx', ["key", "key2"])
        # TODO: How would you test that an index is getting used for an SQL
        #       request?
        self.assertEqual(["key", "key2"],
                         db._get_index_definition('test-idx'))

    def test_list_index_mixed(self):
        db = self.create_database(':memory:')
        # Make sure that we properly order the output
        c = db._get_sqlite_handle().cursor()
        # We intentionally insert the data in weird ordering, to make sure the
        # query still gets it back correctly.
        c.executemany("INSERT INTO index_definitions VALUES (?, ?, ?)",
                      [('idx-1', 0, 'key10'),
                       ('idx-2', 2, 'key22'),
                       ('idx-1', 1, 'key11'),
                       ('idx-2', 0, 'key20'),
                       ('idx-2', 1, 'key21')])
        self.assertEqual([('idx-1', ['key10', 'key11']),
                          ('idx-2', ['key20', 'key21', 'key22'])],
                         db.list_indexes())

    def test_create_extracts_fields(self):
        db = self.create_database(':memory:')
        doc1_id, doc1_rev = db.create_doc('{"key1": "val1", "key2": "val2"}')
        doc2_id, doc2_rev = db.create_doc('{"key1": "valx", "key2": "valy"}')
        c = db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, "key1", "val1"),
                          (doc1_id, "key2", "val2"),
                          (doc2_id, "key1", "valx"),
                          (doc2_id, "key2", "valy"),
                         ], c.fetchall())

    def test_put_updates_fields(self):
        db = self.create_database(':memory:')
        doc1_id, doc1_rev = db.create_doc('{"key1": "val1", "key2": "val2"}')
        doc2_rev = db.put_doc(doc1_id, doc1_rev, '{"key1": "val1", "key2": "valy"}')
        c = db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, "key1", "val1"),
                          (doc1_id, "key2", "valy"),
                         ], c.fetchall())


class TestSQLiteDatabase(SQLiteDatabaseTests, tests.TestCase):

    def create_database(self, fname):
        return sqlite_backend.SQLiteDatabase(fname)


class TestCDatabase(SQLiteDatabaseTests, tests.TestCase):

    def create_database(self, fname):
        return c_wrapper.CDatabase(fname)
