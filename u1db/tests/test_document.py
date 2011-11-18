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
