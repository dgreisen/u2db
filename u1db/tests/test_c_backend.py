# Copyright 2011-2012 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.


from u1db import tests
try:
    from u1db.tests import c_backend_wrapper
except ImportError:
    c_backend_wrapper = None


if c_backend_wrapper is None:
    # Rather than failing all the related tests, just add one test that
    # indicates the backend isn't available
    class NoCDatabase(tests.TestCase):
        def test_exists(self):
            self.fail("Could not import the c_backend_wrapper module."
                      " Was it compiled properly?")
else:
    class TestCDatabase(tests.TestCase):

        def test_exists(self):
            if c_backend_wrapper is None:
                self.fail("Could not import the c_backend_wrapper module."
                          " Was it compiled properly?")
            db = c_backend_wrapper.CDatabase(':memory:')
            self.assertEqual(':memory:', db._filename)

        def test__is_closed(self):
            db = c_backend_wrapper.CDatabase(':memory:')
            self.assertTrue(db._sql_is_open())
            db._close_sqlite_handle()
            self.assertFalse(db._sql_is_open())

        def test__run_sql(self):
            db = c_backend_wrapper.CDatabase(':memory:')
            self.assertTrue(db._sql_is_open())
            self.assertEqual([], db._run_sql('CREATE TABLE test (id INTEGER)'))
            self.assertEqual([], db._run_sql('INSERT INTO test VALUES (1)'))
            self.assertEqual([('1',)], db._run_sql('SELECT * FROM test'))


