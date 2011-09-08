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

"""Test sqlite backend internals."""

from sqlite3 import dbapi2

from u1db import (
    tests,
    )
from u1db.backends import sqlite_backend


simple_doc = '{"key": "value"}'


class TestSQLiteDatabase(tests.TestCase):

    def test_create_database(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        raw_db = db._get_sqlite_handle()
        self.assertNotEqual(None, raw_db)

    def test__close_sqlite_handle(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        raw_db = db._get_sqlite_handle()
        db._close_sqlite_handle()
        self.assertRaises(dbapi2.ProgrammingError,
            raw_db.cursor)

    def test_create_database_initializes_schema(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        raw_db = db._get_sqlite_handle()
        c = raw_db.cursor()
        c.execute("SELECT * FROM u1db_config")
        config = dict([(r[0], r[1]) for r in c.fetchall()])
        self.assertEqual({'sql_schema': '0'}, config)

        # These tables must exist, though we don't care what is in them yet
        c.execute("SELECT * FROM transaction_log")
        c.execute("SELECT * FROM document")
        c.execute("SELECT * FROM document_fields")
        c.execute("SELECT * FROM sync_log")
        c.execute("SELECT * FROM conflicts")
        c.execute("SELECT * FROM index_definitions")

    def test__set_machine_id(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        self.assertEqual(None, db._real_machine_id)
        self.assertEqual(None, db._machine_id)
        db._set_machine_id('foo')
        c = db._get_sqlite_handle().cursor()
        c.execute("SELECT value FROM u1db_config WHERE name='machine_id'")
        self.assertEqual(('foo',), c.fetchone())
        self.assertEqual('foo', db._real_machine_id)
        self.assertEqual('foo', db._machine_id)
        db._close_sqlite_handle()
        self.assertEqual('foo', db._machine_id)

    def test__get_db_rev(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        db._set_machine_id('foo')
        self.assertEqual(0, db._get_db_rev())

    def test__allocate_doc_id(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        self.assertEqual('doc-0', db._allocate_doc_id())

    def test_create_index(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        db.create_index('test-idx', ["key"])
        self.assertEqual([('test-idx', ["key"])], db.list_indexes())

    def test_create_index_multiple_fields(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        db.create_index('test-idx', ["key", "key2"])
        self.assertEqual([('test-idx', ["key", "key2"])], db.list_indexes())

    def test_list_index_mixed(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
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
