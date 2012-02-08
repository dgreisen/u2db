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
from u1db.tests import c_backend_wrapper, c_backend_error


class TestCDatabaseExists(tests.TestCase):

    def test_exists(self):
        if c_backend_wrapper is None:
            self.fail("Could not import the c_backend_wrapper module."
                      " Was it compiled properly?\n%s" % (c_backend_error,))


# Rather than lots of failing tests, we have the above check to test that the
# module exists, and all these tests just get skipped
class BackendTests(tests.TestCase):

    def setUp(self):
        super(BackendTests, self).setUp()
        if c_backend_wrapper is None:
            self.skipTest("The c_backend_wrapper could not be imported")


class TestCDatabase(BackendTests):

    def test_exists(self):
        if c_backend_wrapper is None:
            self.fail("Could not import the c_backend_wrapper module."
                      " Was it compiled properly?")
        db = c_backend_wrapper.CDatabase(':memory:')
        self.assertEqual(':memory:', db._filename)

    def test__is_closed(self):
        db = c_backend_wrapper.CDatabase(':memory:')
        self.assertTrue(db._sql_is_open())
        db.close()
        self.assertFalse(db._sql_is_open())

    def test__run_sql(self):
        db = c_backend_wrapper.CDatabase(':memory:')
        self.assertTrue(db._sql_is_open())
        self.assertEqual([], db._run_sql('CREATE TABLE test (id INTEGER)'))
        self.assertEqual([], db._run_sql('INSERT INTO test VALUES (1)'))
        self.assertEqual([('1',)], db._run_sql('SELECT * FROM test'))

    def test__set_replica_uid(self):
        db = c_backend_wrapper.CDatabase(':memory:')
        self.assertIsNot(None, db._replica_uid)
        db._set_replica_uid('foo')
        self.assertEqual([('foo',)], db._run_sql(
            "SELECT value FROM u1db_config WHERE name='replica_uid'"))

    def test_default_replica_uid(self):
        self.db = c_backend_wrapper.CDatabase(':memory:')
        self.assertIsNot(None, self.db._replica_uid)
        self.assertEqual(32, len(self.db._replica_uid))
        val = int(self.db._replica_uid, 16)

    def test_get_conflicts_with_borked_data(self):
        self.db = c_backend_wrapper.CDatabase(':memory:')
        # We add an entry to conflicts, but not to documents, which is an
        # invalid situation
        self.db._run_sql("INSERT INTO conflicts"
                         " VALUES ('doc-id', 'doc-rev', '{}')")
        self.assertRaises(Exception, self.db.get_doc_conflicts, 'doc-id')


class TestVectorClock(BackendTests):

    def create_vcr(self, rev):
        return c_backend_wrapper.VectorClockRev(rev)

    def test_parse_empty(self):
        self.assertEqual('VectorClockRev()',
                         repr(self.create_vcr('')))

    def test_parse_invalid(self):
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('x')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('x:a')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|x:a')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('x:a|y:1')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|x:2a')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1||')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|x:2|')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|x:2|:')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|x:2|m:')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|x:|m:3')))
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('y:1|:|m:3')))

    def test_parse_single(self):
        self.assertEqual('VectorClockRev(test:1)',
                         repr(self.create_vcr('test:1')))

    def test_parse_multi(self):
        self.assertEqual('VectorClockRev(test:1|z:2)',
                         repr(self.create_vcr('test:1|z:2')))
        self.assertEqual('VectorClockRev(ab:1|bc:2|cd:3|de:4|ef:5)',
                     repr(self.create_vcr('ab:1|bc:2|cd:3|de:4|ef:5')))

    def test_unsorted(self):
        # TODO: Eventually this should just handle the unsorted case, and fix
        # it.
        self.assertEqual('VectorClockRev(None)',
                         repr(self.create_vcr('b:1|a:2')))


class TestCDocument(BackendTests):

    def make_document(self, *args, **kwargs):
        return c_backend_wrapper.make_document(*args, **kwargs)

    def test_create(self):
        doc = self.make_document('doc-id', 'uid:1', tests.simple_doc)


class TestUUID(BackendTests):

    def test_uuid4_conformance(self):
        uuids = set()
        for i in range(20):
            uuid = c_backend_wrapper.generate_hex_uuid()
            self.assertIsInstance(uuid, str)
            self.assertEqual(32, len(uuid))
            # This will raise ValueError if it isn't a valid hex string
            v = long(uuid, 16)
            # Version 4 uuids have 2 other requirements, the high 4 bits of the
            # seventh byte are always '0x4', and the middle bits of byte 9 are
            # always set
            self.assertEqual('4', uuid[12])
            self.assertTrue(uuid[16] in '89ab')
            self.assertTrue(uuid not in uuids)
            uuids.add(uuid)