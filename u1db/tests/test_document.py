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

    def test_create_doc(self):
        doc = Document('doc-id', 'uid:1', tests.simple_doc)
        self.assertEqual('doc-id', doc.doc_id)
        self.assertEqual('uid:1', doc.rev)
        self.assertEqual(tests.simple_doc, doc.content)

    def test__repr__(self):
        doc = Document('doc-id', 'uid:1', tests.simple_doc)
        self.assertEqual('Document(doc-id, uid:1, \'{"key": "value"}\')',
                         repr(doc))

    def test__repr__conflicted(self):
        doc = Document('doc-id', 'uid:1', tests.simple_doc, has_conflicts=True)
        self.assertEqual(
            'Document(doc-id, uid:1, conflicted, \'{"key": "value"}\')',
            repr(doc))

    def test__lt__(self):
        doc_a = Document('a', 'b', 'c')
        doc_b = Document('b', 'b', 'c')
        self.assertTrue(doc_a < doc_b)
        self.assertTrue(doc_b > doc_a)
        doc_aa = Document('a', 'a', 'b')
        self.assertTrue(doc_aa < doc_a)
