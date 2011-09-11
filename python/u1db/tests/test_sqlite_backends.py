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

    def setUp(self):
        super(SQLiteDatabaseTests, self).setUp()
        self.db = self.create_database(':memory:')

    def test_create_database_initializes_schema(self):
        config = dict([(r[0], r[1])
                      for r in self.db._run_sql("SELECT * FROM u1db_config")])
        self.assertEqual({'sql_schema': '0'}, config)

        # These tables must exist, though we don't care what is in them yet
        self.db._run_sql("SELECT * FROM transaction_log")
        self.db._run_sql("SELECT * FROM document")
        self.db._run_sql("SELECT * FROM document_fields")
        self.db._run_sql("SELECT * FROM sync_log")
        self.db._run_sql("SELECT * FROM conflicts")
        self.db._run_sql("SELECT * FROM index_definitions")

    def test__set_machine_id(self):
        self.assertEqual(None, self.db._machine_id)
        self.db._set_machine_id('foo')
        res = self.db._run_sql(
            "SELECT value FROM u1db_config WHERE name='machine_id'")
        self.assertEqual([('foo',)], res)
        self.db._close_sqlite_handle()
        self.assertEqual('foo', self.db._machine_id)

    def test__get_db_rev(self):
        self.db._set_machine_id('foo')
        self.assertEqual(0, self.db._get_db_rev())

    def test__allocate_doc_id(self):
        self.assertEqual('doc-0', self.db._allocate_doc_id())

    def test_create_doc(self):
        self.db._set_machine_id('test')
        doc = '{"key": "value"}'
        doc_id, new_rev = self.db.create_doc(doc)
        self.assertNotEqual(None, doc_id)
        self.assertNotEqual(None, new_rev)
        self.assertEqual([(doc_id, new_rev, doc)],
            self.db._run_sql("SELECT doc_id, doc_rev, doc FROM document"))
        self.assertEqual((new_rev, simple_doc, False), self.db.get_doc(doc_id))

    def test_create_index(self):
        if not self.db._supports_indexes:
            self.skipTest('db %s does not support indexes' % (self.db,))
        self.db.create_index('test-idx', ["key"])
        self.assertEqual([('test-idx', ["key"])], self.db.list_indexes())

    def test_create_index_multiple_fields(self):
        if not self.db._supports_indexes:
            self.skipTest('db %s does not support indexes' % (self.db,))
        self.db.create_index('test-idx', ["key", "key2"])
        self.assertEqual([('test-idx', ["key", "key2"])], self.db.list_indexes())

    def test__get_index_definition(self):
        if not self.db._supports_indexes:
            self.skipTest('db %s does not support indexes' % (self.db,))
        self.db.create_index('test-idx', ["key", "key2"])
        # TODO: How would you test that an index is getting used for an SQL
        #       request?
        self.assertEqual(["key", "key2"],
                         self.db._get_index_definition('test-idx'))

    def test_create_extracts_fields(self):
        if not self.db._supports_indexes:
            self.skipTest('db %s does not support indexes' % (self.db,))
        doc1_id, doc1_rev = self.db.create_doc('{"key1": "val1", "key2": "val2"}')
        doc2_id, doc2_rev = self.db.create_doc('{"key1": "valx", "key2": "valy"}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, "key1", "val1"),
                          (doc1_id, "key2", "val2"),
                          (doc2_id, "key1", "valx"),
                          (doc2_id, "key2", "valy"),
                         ], c.fetchall())

    def test_put_updates_fields(self):
        if not self.db._supports_indexes:
            self.skipTest('db %s does not support indexes' % (self.db,))
        doc1_id, doc1_rev = self.db.create_doc('{"key1": "val1", "key2": "val2"}')
        doc2_rev = self.db.put_doc(doc1_id, doc1_rev,
                        '{"key1": "val1", "key2": "valy"}')
        self.assertEqual([(doc1_id, "key1", "val1"),
                          (doc1_id, "key2", "valy")],
            self.db._run_sql("SELECT doc_id, field_name, value"
                             "  FROM document_fields"
                             " ORDER BY doc_id, field_name, value"))


class TestSQLiteDatabase(SQLiteDatabaseTests, tests.TestCase):

    def create_database(self, fname):
        return sqlite_backend.SQLiteDatabase(fname)


class TestCDatabase(SQLiteDatabaseTests, tests.TestCase):

    def create_database(self, fname):
        return c_wrapper.CDatabase(fname)
