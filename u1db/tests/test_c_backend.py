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

    def test_get_from_index(self):
        # We manually poke data into the DB, so that we test just the "get_doc"
        # code, rather than also testing the index management code.
        self.db = c_backend_wrapper.CDatabase(':memory:')
        doc = self.db.create_doc(tests.simple_doc)
        self.db.create_index("key-idx", ["key"])
        docs = self.db.get_from_index('key-idx', [('value',)])
        self.assertEqual([doc], docs)

    def test_get_from_index_2(self):
        self.db = c_backend_wrapper.CDatabase(':memory:')
        doc = self.db.create_doc(tests.nested_doc)
        self.db.create_index("multi-idx", ["key", "sub.doc"])
        # TODO: The current backend doesn't support nested fields, so we push
        #       that data in manually.
        self.db._run_sql("INSERT INTO document_fields"
                         " VALUES ('%s', 'sub.doc', 'underneath')"
                         % (doc.doc_id,))
        docs = self.db.get_from_index('multi-idx', [('value', 'underneath')])
        self.assertEqual([doc], docs)

    def test__query_init_one_field(self):
        self.db = c_backend_wrapper.CDatabase(':memory:')
        self.db.create_index("key-idx", ["key"])
        query = self.db._query_init("key-idx")
        self.assertEqual("key-idx", query.index_name)
        self.assertEqual(1, query.num_fields)
        self.assertEqual(["key"], query.fields)

    def test__query_init_two_fields(self):
        self.db = c_backend_wrapper.CDatabase(':memory:')
        self.db.create_index("two-idx", ["key", "key2"])
        query = self.db._query_init("two-idx")
        self.assertEqual("two-idx", query.index_name)
        self.assertEqual(2, query.num_fields)
        self.assertEqual(["key", "key2"], query.fields)

    def assertFormatQueryEquals(self, expected, wildcards, fields):
        val, w = c_backend_wrapper._format_query(fields)
        self.assertEqual(expected, val)
        self.assertEqual(wildcards, w)

    def test__format_query(self):
        self.assertFormatQueryEquals(
            "SELECT d0.doc_id FROM document_fields d0"
            " WHERE d0.field_name = ? AND d0.value = ?",
            [0], ["1"])
        self.assertFormatQueryEquals(
            "SELECT d0.doc_id"
            " FROM document_fields d0, document_fields d1"
            " WHERE d0.field_name = ? AND d0.value = ?"
            " AND d0.doc_id = d1.doc_id"
            " AND d1.field_name = ? AND d1.value = ?",
            [0, 0], ["1", "2"])
        self.assertFormatQueryEquals(
            "SELECT d0.doc_id"
            " FROM document_fields d0, document_fields d1, document_fields d2"
            " WHERE d0.field_name = ? AND d0.value = ?"
            " AND d0.doc_id = d1.doc_id"
            " AND d1.field_name = ? AND d1.value = ?"
            " AND d0.doc_id = d2.doc_id"
            " AND d2.field_name = ? AND d2.value = ?",
            [0, 0, 0], ["1", "2", "3"])

    def test__format_query_wildcard(self):
        self.assertFormatQueryEquals(
            "SELECT d0.doc_id FROM document_fields d0"
            " WHERE d0.field_name = ? AND d0.value NOT NULL",
            [1], ["*"])
        self.assertFormatQueryEquals(
            "SELECT d0.doc_id"
            " FROM document_fields d0, document_fields d1"
            " WHERE d0.field_name = ? AND d0.value = ?"
            " AND d0.doc_id = d1.doc_id"
            " AND d1.field_name = ? AND d1.value NOT NULL",
            [0, 1], ["1", "*"])

    def test__format_query_glob(self):
        self.assertRaises(NotImplementedError,
            c_backend_wrapper._format_query, ["1*"])


class TestCSyncTarget(BackendTests):

    def setUp(self):
        super(TestCSyncTarget, self).setUp()
        self.db = c_backend_wrapper.CDatabase(':memory:')
        self.st = self.db.get_sync_target()

    def test_attached_to_db(self):
        self.assertEqual(self.db._replica_uid, self.st.get_sync_info("misc")[0])

    def test_get_sync_exchange(self):
        exc = self.st._get_sync_exchange("source-uid", 10)
        self.assertIsNot(None, exc)

    def test_sync_exchange_insert_doc_from_source(self):
        exc = self.st._get_sync_exchange("source-uid", 5)
        doc = c_backend_wrapper.make_document('doc-id', 'replica:1',
                tests.simple_doc)
        self.assertEqual([], exc.get_seen_ids())
        exc.insert_doc_from_source(doc, 10)
        self.assertGetDoc(self.db, 'doc-id', 'replica:1', tests.simple_doc,
                          False)
        self.assertEqual(10, self.db.get_sync_generation('source-uid'))
        self.assertEqual(['doc-id'], exc.get_seen_ids())

    def test_sync_exchange_conflicted_doc(self):
        doc = self.db.create_doc(tests.simple_doc)
        exc = self.st._get_sync_exchange("source-uid", 5)
        doc2 = c_backend_wrapper.make_document(doc.doc_id, 'replica:1',
                tests.nested_doc)
        self.assertEqual([], exc.get_seen_ids())
        # The insert should be rejected and the doc_id not considered 'seen'
        exc.insert_doc_from_source(doc2, 10)
        self.assertGetDoc(self.db, doc.doc_id, doc.rev, tests.simple_doc, False)
        self.assertEqual([], exc.get_seen_ids())

    def test_sync_exchange_find_doc_ids(self):
        doc = self.db.create_doc(tests.simple_doc)
        exc = self.st._get_sync_exchange("source-uid", 5)
        self.assertEqual(0, exc.new_gen)
        exc.find_doc_ids_to_return()
        self.assertEqual([(doc.doc_id, 1)], exc.get_doc_ids_to_return())
        self.assertEqual(1, exc.new_gen)

    def test_sync_exchange_find_doc_ids_not_including_recently_inserted(self):
        doc1 = self.db.create_doc(tests.simple_doc)
        doc2 = self.db.create_doc(tests.nested_doc)
        exc = self.st._get_sync_exchange("source-uid", 5)
        doc3 = c_backend_wrapper.make_document(doc1.doc_id,
                doc1.rev + "|zreplica:2", tests.simple_doc)
        exc.insert_doc_from_source(doc3, 10)
        exc.find_doc_ids_to_return()
        self.assertEqual([(doc2.doc_id, 2)], exc.get_doc_ids_to_return())
        self.assertEqual(3, exc.new_gen)

    def test_sync_exchange_return_docs(self):
        returned = []
        def return_doc_cb(doc, gen):
            returned.append((doc, gen))
        doc1 = self.db.create_doc(tests.simple_doc)
        exc = self.st._get_sync_exchange("source-uid", 5)
        exc.find_doc_ids_to_return()
        exc.return_docs(return_doc_cb)
        self.assertEqual([(doc1, 1)], returned)


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
        self.assertEqual('VectorClockRev(a:2|b:1)',
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
