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
        self.assertEqual([], db._run_sql('CREATE TABLE test (id INTEGER)'))
        self.assertEqual([], db._run_sql('INSERT INTO test VALUES (1)'))
        self.assertEqual([('1',)], db._run_sql('SELECT * FROM test'))


class TestVectorClock(tests.TestCase):

    def test_parse_empty(self):
        self.assertEqual('VectorClock()',
                         repr(c_wrapper.VectorClock('')))

    def test_parse_invalid(self):
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('x')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('x:a')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|x:a')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('x:a|y:1')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|x:2a')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1||')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|x:2|')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|x:2|:')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|x:2|m:')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|x:|m:3')))
        self.assertEqual('VectorClock(None)',
                         repr(c_wrapper.VectorClock('y:1|:|m:3')))

    def test_parse_single(self):
        self.assertEqual('VectorClock(test:1)',
                         repr(c_wrapper.VectorClock('test:1')))

    def test_parse_multi(self):
        self.assertEqual('VectorClock(test:1|z:2)',
                         repr(c_wrapper.VectorClock('test:1|z:2')))
        self.assertEqual('VectorClock(ab:1|bc:2|cd:3|de:4|ef:5)',
                     repr(c_wrapper.VectorClock('ab:1|bc:2|cd:3|de:4|ef:5')))

    def test_increment(self):
        vc = c_wrapper.VectorClock('test:1')
        vc.increment('test')
        self.assertEqual('VectorClock(test:2)', repr(vc))

    def test_increment_with_multi(self):
        vc = c_wrapper.VectorClock('a:1|ab:2')
        vc.increment('a')
        self.assertEqual('VectorClock(a:2|ab:2)', repr(vc))
        vc.increment('ab')
        self.assertEqual('VectorClock(a:2|ab:3)', repr(vc))

    def test_increment_insert_new_id(self):
        vc = c_wrapper.VectorClock('a:1|ab:2')
        vc.increment('aa')
        self.assertEqual('VectorClock(a:1|aa:1|ab:2)', repr(vc))

    def test_increment_first_id(self):
        vc = c_wrapper.VectorClock('b:2')
        vc.increment('a')
        self.assertEqual('VectorClock(a:1|b:2)', repr(vc))

    def test_increment_append_id(self):
        vc = c_wrapper.VectorClock('b:2')
        vc.increment('c')
        self.assertEqual('VectorClock(b:2|c:1)', repr(vc))
