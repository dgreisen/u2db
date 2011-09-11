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

    def test__set_machine_id(self):
        db = sqlite_backend.SQLiteDatabase(':memory:')
        self.assertEqual(None, db._real_machine_id)
        self.assertEqual(None, db._machine_id)
        db._set_machine_id('foo')
        res = db._run_sql("SELECT value FROM u1db_config WHERE name='machine_id'")
        self.assertEqual(('foo',), res[0])
        self.assertEqual('foo', db._real_machine_id)
        self.assertEqual('foo', db._machine_id)
        db._close_sqlite_handle()
        self.assertEqual('foo', db._machine_id)

    def test_list_index_mixed(self):
        # TODO: Rewrite this test so that it can work against the CWrapper.
        #       Most likely this means doing the value bindings ourselves.
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

