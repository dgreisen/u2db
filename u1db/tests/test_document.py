# Copyright 2011 Canonical Ltd.
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


from u1db import Document
from u1db import tests


class TestDocument(tests.TestCase):

    scenarios = ([('py', {'make_document': Document})] +
                 tests.C_DATABASE_SCENARIOS)

    def test_create_doc(self):
        doc = self.make_document('doc-id', 'uid:1', tests.simple_doc)
        self.assertEqual('doc-id', doc.doc_id)
        self.assertEqual('uid:1', doc.rev)
        self.assertEqual(tests.simple_doc, doc.get_json())
        self.assertFalse(doc.has_conflicts)

    def test__repr__(self):
        doc = self.make_document('doc-id', 'uid:1', tests.simple_doc)
        self.assertEqual(
            '%s(doc-id, uid:1, \'{"key": "value"}\')'
                % (doc.__class__.__name__,),
            repr(doc))

    def test__repr__conflicted(self):
        doc = self.make_document('doc-id', 'uid:1', tests.simple_doc,
                                 has_conflicts=True)
        self.assertEqual(
            '%s(doc-id, uid:1, conflicted, \'{"key": "value"}\')'
                % (doc.__class__.__name__,),
            repr(doc))

    def test__lt__(self):
        doc_a = self.make_document('a', 'b', '{}')
        doc_b = self.make_document('b', 'b', '{}')
        self.assertTrue(doc_a < doc_b)
        self.assertTrue(doc_b > doc_a)
        doc_aa = self.make_document('a', 'a', '{}')
        self.assertTrue(doc_aa < doc_a)

    def test__eq__(self):
        doc_a = self.make_document('a', 'b', '{}')
        doc_b = self.make_document('a', 'b', '{}')
        self.assertTrue(doc_a == doc_b)
        doc_b = self.make_document('a', 'b', '{}', has_conflicts=True)
        self.assertFalse(doc_a == doc_b)

    def test_set_json(self):
        doc = self.make_document('id', 'rev', '{"content":""}')
        self.assertEqual('{"content":""}', doc.get_json())
        doc.set_json('{"content": "new"}')
        self.assertEqual('{"content": "new"}', doc.get_json())


class TestPyDocument(tests.TestCase):

    scenarios = ([('py', {'make_document': Document})])

    def test_get_content(self):
        doc = self.make_document('id', 'rev', '{"content":""}')
        self.assertEqual({"content": ""}, doc.content)
        doc.set_json('{"content": "new"}')
        self.assertEqual({"content": "new"}, doc.content)

    def test_set_content(self):
        doc = self.make_document('id', 'rev', '{"content":""}')
        doc.content = {"content": "new"}
        self.assertEqual('{"content": "new"}', doc.get_json())

    def test_is_deleted(self):
        doc_a = self.make_document('a', 'b', '{}')
        self.assertFalse(doc_a.is_deleted())
        doc_a.set_json(None)
        self.assertTrue(doc_a.is_deleted())


load_tests = tests.load_with_scenarios
