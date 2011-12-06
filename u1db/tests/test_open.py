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

"""Test u1db.open"""

import os

from u1db import (
    errors,
    open as u1db_open,
    tests,
    )
from u1db.backends import sqlite_backend


class TestU1DBOpen(tests.TestCase):

    def setUp(self):
        super(TestU1DBOpen, self).setUp()
        tmpdir = self.createTempDir()
        self.db_path = tmpdir + '/test.db'

    def test_open_no_create(self):
        self.assertRaises(errors.DatabaseDoesNotExist,
                          u1db_open, self.db_path, create=False)
        self.assertFalse(os.path.exists(self.db_path))

    def test_open_create(self):
        db = u1db_open(self.db_path, create=True)
        self.addCleanup(db.close)
        self.assertTrue(os.path.exists(self.db_path))
        self.assertIsInstance(db, sqlite_backend.SQLiteDatabase)

    def test_open_existing(self):
        db = sqlite_backend.SQLitePartialExpandDatabase(self.db_path)
        self.addCleanup(db.close)
        doc = db.create_doc(tests.simple_doc)
        # Even though create=True, we shouldn't wipe the db
        db2 = u1db_open(self.db_path, create=True)
        self.addCleanup(db2.close)
        doc2 = db2.get_doc(doc.doc_id)
        self.assertEqual(doc, doc2)

    def test_open_existing_no_create(self):
        db = sqlite_backend.SQLitePartialExpandDatabase(self.db_path)
        self.addCleanup(db.close)
        db2 = u1db_open(self.db_path, create=False)
        self.addCleanup(db2.close)
        self.assertIsInstance(db2, sqlite_backend.SQLitePartialExpandDatabase)
