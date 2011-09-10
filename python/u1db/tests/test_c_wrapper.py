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

"""Tests for the wrapper around the C implementation."""

from u1db import (
    tests,
    )
from u1db.backends import c_wrapper


class TestCWrapper(tests.TestCase):

    def test_exists(self):
        db = c_wrapper.CDatabase(':memory:')
        self.assertEqual(':memory:', db._filename)

    def test__is_closed(self):
        db = c_wrapper.CDatabase(':memory:')
        self.assertTrue(db._sql_is_open())
        db._close_sqlite_handle()
        self.assertFalse(db._sql_is_open())

    def test__run_sql(self):
        db = c_wrapper.CDatabase(':memory:')
        self.assertTrue(db._sql_is_open())
        self.assertEqual((0, []), db._run_sql('CREATE TABLE test (id INTEGER)'))
        self.assertEqual((0, []), db._run_sql('INSERT INTO test VALUES (1)'))
        self.assertEqual((0, [['1']]), db._run_sql('SELECT * FROM test'))
