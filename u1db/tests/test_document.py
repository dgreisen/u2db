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

try:
    from u1db.tests import c_backend_wrapper
except ImportError:
    c_backend_wrapper = None


if c_backend_wrapper is None:
    c_scenarios = []
else:
    c_scenarios = [('c', {'make_document': c_backend_wrapper.CDocument})]


class TestDocument(tests.TestCase):

    scenarios = [('py', {'make_document': Document})] + c_scenarios

    def test_create_doc(self):
        doc = self.make_document('doc-id', 'uid:1', tests.simple_doc)
        self.assertEqual('doc-id', doc.doc_id)
        self.assertEqual('uid:1', doc.rev)
        self.assertEqual(tests.simple_doc, doc.content)
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
        doc_a = self.make_document('a', 'b', 'c')
        doc_b = self.make_document('b', 'b', 'c')
        self.assertTrue(doc_a < doc_b)
        self.assertTrue(doc_b > doc_a)
        doc_aa = self.make_document('a', 'a', 'b')
        self.assertTrue(doc_aa < doc_a)

    def test__eq__(self):
        doc_a = self.make_document('a', 'b', 'c')
        doc_b = self.make_document('a', 'b', 'c')
        self.assertTrue(doc_a == doc_b)
        doc_b = self.make_document('a', 'b', 'c', has_conflicts=True)
        self.assertFalse(doc_a == doc_b)


load_tests = tests.load_with_scenarios
