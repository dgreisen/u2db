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

import tempfile
import shutil

from sqlite3 import dbapi2
import simplejson

from u1db import (
    tests,
    )
from u1db.backends import sqlite_backend


simple_doc = '{"key": "value"}'
nested_doc = '{"key": "value", "sub": {"doc": "underneath"}}'


class TestSQLiteExpandedDatabase(tests.TestCase):

    def setUp(self):
        super(TestSQLiteExpandedDatabase, self).setUp()
        self.db = sqlite_backend.SQLiteExpandedDatabase(':memory:')
        self.db._set_machine_id('test')

    def test_create_database(self):
        raw_db = self.db._get_sqlite_handle()
        self.assertNotEqual(None, raw_db)

    def test__close_sqlite_handle(self):
        raw_db = self.db._get_sqlite_handle()
        self.db._close_sqlite_handle()
        self.assertRaises(dbapi2.ProgrammingError,
            raw_db.cursor)

    def test_create_database_initializes_schema(self):
        raw_db = self.db._get_sqlite_handle()
        c = raw_db.cursor()
        c.execute("SELECT * FROM u1db_config")
        config = dict([(r[0], r[1]) for r in c.fetchall()])
        self.assertEqual({'sql_schema': '0', 'machine_id': 'test',
                          'index_storage': 'expanded'}, config)

        # These tables must exist, though we don't care what is in them yet
        c.execute("SELECT * FROM transaction_log")
        c.execute("SELECT * FROM document")
        c.execute("SELECT * FROM document_fields")
        c.execute("SELECT * FROM sync_log")
        c.execute("SELECT * FROM conflicts")
        c.execute("SELECT * FROM index_definitions")

    def test__set_machine_id(self):
        # Start from scratch, so that machine_id isn't set.
        self.db = sqlite_backend.SQLiteExpandedDatabase(':memory:')
        self.assertEqual(None, self.db._real_machine_id)
        self.assertEqual(None, self.db._machine_id)
        self.db._set_machine_id('foo')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT value FROM u1db_config WHERE name='machine_id'")
        self.assertEqual(('foo',), c.fetchone())
        self.assertEqual('foo', self.db._real_machine_id)
        self.assertEqual('foo', self.db._machine_id)
        self.db._close_sqlite_handle()
        self.assertEqual('foo', self.db._machine_id)

    def test__get_db_rev(self):
        self.db._set_machine_id('foo')
        self.assertEqual(0, self.db._get_db_rev())

    def test__allocate_doc_id(self):
        self.assertEqual('doc-0', self.db._allocate_doc_id())

    def test_create_index(self):
        self.db.create_index('test-idx', ["key"])
        self.assertEqual([('test-idx', ["key"])], self.db.list_indexes())

    def test_create_index_multiple_fields(self):
        self.db.create_index('test-idx', ["key", "key2"])
        self.assertEqual([('test-idx', ["key", "key2"])],
                         self.db.list_indexes())

    def test__get_index_definition(self):
        self.db.create_index('test-idx', ["key", "key2"])
        # TODO: How would you test that an index is getting used for an SQL
        #       request?
        self.assertEqual(["key", "key2"],
                         self.db._get_index_definition('test-idx'))

    def test_list_index_mixed(self):
        # Make sure that we properly order the output
        c = self.db._get_sqlite_handle().cursor()
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
                         self.db.list_indexes())

    def test_create_extracts_fields(self):
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
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        doc2_rev = self.db.put_doc(doc1_id, doc1_rev,
            '{"key1": "val1", "key2": "valy"}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, "key1", "val1"),
                          (doc1_id, "key2", "valy"),
                         ], c.fetchall())

    def test_put_updates_nested_fields(self):
        doc1_id, doc1_rev = self.db.create_doc(nested_doc)
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, "key", "value"),
                          (doc1_id, "sub.doc", "underneath"),
                         ], c.fetchall())

    def test_open_database(self):
        temp_dir = tempfile.mkdtemp(prefix='u1db-test-')
        self.addCleanup(shutil.rmtree, temp_dir)
        path = temp_dir + '/test.sqlite'
        db = sqlite_backend.SQLiteExpandedDatabase(path)
        db2 = sqlite_backend.SQLiteDatabase.open_database(path)
        self.assertIsInstance(db2, sqlite_backend.SQLiteExpandedDatabase)


class TestSQLitePartialExpandDatabase(tests.TestCase):

    def setUp(self):
        super(TestSQLitePartialExpandDatabase, self).setUp()
        self.db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
        self.db._set_machine_id('test')

    def test_u1db_config_settings(self):
        raw_db = self.db._get_sqlite_handle()
        c = raw_db.cursor()
        c.execute("SELECT * FROM u1db_config")
        config = dict([(r[0], r[1]) for r in c.fetchall()])
        self.assertEqual({'sql_schema': '0', 'machine_id': 'test',
                          'index_storage': 'expand referenced'}, config)

    def test_no_indexes_no_document_fields(self):
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([], c.fetchall())

    def test__get_indexed_fields(self):
        self.db.create_index('idx1', ['a', 'b'])
        self.assertEqual(set(['a', 'b']), self.db._get_indexed_fields())
        self.db.create_index('idx2', ['b', 'c'])
        self.assertEqual(set(['a', 'b', 'c']), self.db._get_indexed_fields())

    def test_indexed_fields_expanded(self):
        self.db.create_index('idx1', ['key1'])
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        self.assertEqual(set(['key1']), self.db._get_indexed_fields())
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, 'key1', 'val1')], c.fetchall())

    def test_create_index_updates_fields(self):
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        self.db.create_index('idx1', ['key1'])
        self.assertEqual(set(['key1']), self.db._get_indexed_fields())
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([(doc1_id, 'key1', 'val1')], c.fetchall())

    def test_open_database(self):
        temp_dir = tempfile.mkdtemp(prefix='u1db-test-')
        self.addCleanup(shutil.rmtree, temp_dir)
        path = temp_dir + '/test.sqlite'
        db = sqlite_backend.SQLitePartialExpandDatabase(path)
        db2 = sqlite_backend.SQLiteDatabase.open_database(path)
        self.assertIsInstance(db2, sqlite_backend.SQLitePartialExpandDatabase)


class TestSQLiteOnlyExpandedDatabase(tests.TestCase):

    def setUp(self):
        super(TestSQLiteOnlyExpandedDatabase, self).setUp()
        self.db = sqlite_backend.SQLiteOnlyExpandedDatabase(':memory:')
        self.db._set_machine_id('test')

    def test_u1db_config_settings(self):
        raw_db = self.db._get_sqlite_handle()
        c = raw_db.cursor()
        c.execute("SELECT * FROM u1db_config")
        config = dict([(r[0], r[1]) for r in c.fetchall()])
        self.assertEqual({'sql_schema': '0', 'machine_id': 'test',
                          'index_storage': 'only expanded'}, config)

    def test_no_document_content(self):
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, doc FROM document ORDER BY doc_id")
        self.assertEqual([(doc1_id, None)], c.fetchall())
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name")
        self.assertEqual([(doc1_id, 'key1', 'val1'),
                          (doc1_id, 'key2', 'val2'),
                         ], c.fetchall())

    def test_get_doc_reassembles_content(self):
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        self.assertEqual((doc1_rev, '{"key1": "val1", "key2": "val2"}', False),
                         self.db.get_doc(doc1_id))

    def test_distinguish_deleted_from_empty_doc(self):
        doc1_id, doc1_rev = self.db.create_doc('{}')
        self.assertEqual((doc1_rev, '{}', False), self.db.get_doc(doc1_id))
        doc1_rev2 = self.db.delete_doc(doc1_id, doc1_rev)
        self.assertEqual((doc1_rev2, None, False), self.db.get_doc(doc1_id))

    def test_deeply_nested(self):
        doc1_id, doc1_rev = self.db.create_doc(
            '{"a": {"b": {"c": {"d": "x"}}}}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name")
        self.assertEqual([(doc1_id, 'a.b.c.d', 'x'),
                         ], c.fetchall())

    def test_open_database(self):
        temp_dir = tempfile.mkdtemp(prefix='u1db-test-')
        self.addCleanup(shutil.rmtree, temp_dir)
        path = temp_dir + '/test.sqlite'
        db = sqlite_backend.SQLiteOnlyExpandedDatabase(path)
        db2 = sqlite_backend.SQLiteDatabase.open_database(path)
        self.assertIsInstance(db2, sqlite_backend.SQLiteOnlyExpandedDatabase)
