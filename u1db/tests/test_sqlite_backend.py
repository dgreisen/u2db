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


class TestSQLitePartialExpandDatabase(tests.TestCase):

    def setUp(self):
        super(TestSQLitePartialExpandDatabase, self).setUp()
        self.db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
        self.db._set_replica_uid('test')

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
        self.assertEqual({'sql_schema': '0', 'replica_uid': 'test',
                          'index_storage': 'expand referenced'}, config)

        # These tables must exist, though we don't care what is in them yet
        c.execute("SELECT * FROM transaction_log")
        c.execute("SELECT * FROM document")
        c.execute("SELECT * FROM document_fields")
        c.execute("SELECT * FROM sync_log")
        c.execute("SELECT * FROM conflicts")
        c.execute("SELECT * FROM index_definitions")

    def test__set_replica_uid(self):
        # Start from scratch, so that replica_uid isn't set.
        self.db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
        self.assertEqual(None, self.db._real_replica_uid)
        self.assertEqual(None, self.db._replica_uid)
        self.db._set_replica_uid('foo')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT value FROM u1db_config WHERE name='replica_uid'")
        self.assertEqual(('foo',), c.fetchone())
        self.assertEqual('foo', self.db._real_replica_uid)
        self.assertEqual('foo', self.db._replica_uid)
        self.db._close_sqlite_handle()
        self.assertEqual('foo', self.db._replica_uid)

    def test__get_generation(self):
        self.db._set_replica_uid('foo')
        self.assertEqual(0, self.db._get_generation())

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

    def test_no_indexes_no_document_fields(self):
        doc1_id, doc1_rev = self.db.create_doc(
            '{"key1": "val1", "key2": "val2"}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([], c.fetchall())

    def test_create_extracts_fields(self):
        doc1_id, doc1_rev = self.db.create_doc('{"key1": "val1", "key2": "val2"}')
        doc2_id, doc2_rev = self.db.create_doc('{"key1": "valx", "key2": "valy"}')
        c = self.db._get_sqlite_handle().cursor()
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual([], c.fetchall())
        self.db.create_index('test', ['key1', 'key2'])
        c.execute("SELECT doc_id, field_name, value FROM document_fields"
                  " ORDER BY doc_id, field_name, value")
        self.assertEqual(sorted(
            [(doc1_id, "key1", "val1"),
             (doc1_id, "key2", "val2"),
             (doc2_id, "key1", "valx"),
             (doc2_id, "key2", "valy"),
            ]), sorted(c.fetchall()))

    def test_put_updates_fields(self):
        self.db.create_index('test', ['key1', 'key2'])
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
        self.db.create_index('test', ['key', 'sub.doc'])
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
        db = sqlite_backend.SQLitePartialExpandDatabase(path)
        db2 = sqlite_backend.SQLiteDatabase.open_database(path)
        self.assertIsInstance(db2, sqlite_backend.SQLitePartialExpandDatabase)

    def assertTransform(self, sql_value, value):
        transformed = sqlite_backend.SQLiteDatabase._transform_glob(value)
        self.assertEqual(sql_value, transformed)

    def test_glob_escaping(self):
        # SQL allows us to define any escape char we want, for now I'm just
        # using '.'
        self.assertTransform('val%', 'val*')
        self.assertTransform('v.%al%', 'v%al*')
        self.assertTransform('v._al%', 'v_al*')
        self.assertTransform('v..al%', 'v.al*')

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
