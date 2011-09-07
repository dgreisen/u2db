# Copyright (C) 2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""The Client class for U1DB."""


from u1dbclient import (
    client,
    tests,
    )


class TestClient(tests.TestCase):

    def test_create(self):
        c = client.Client()

    def test_has_api_sync(self):
        c = client.Client()
        self.assertNotEqual(None, getattr(c, 'sync', None))

    def test_has_api_whatschanged(self):
        c = client.Client()
        self.assertNotEqual(None, getattr(c, 'whats_changed', None))


class TestInMemoryClient(tests.TestCase):

    def setUp(self):
        super(TestInMemoryClient, self).setUp()
        self.c = client.InMemoryClient()

    def test__allocate_doc_id(self):
        self.assertEqual('doc-1', self.c._allocate_doc_id())

    def test__allocate_doc_rev_from_None(self):
        self.assertEqual('test:1', self.c._allocate_doc_rev(None))

    def test__allocate_doc_rev_incremental(self):
        self.assertEqual('test:2', self.c._allocate_doc_rev('test:1'))

    def test__allocate_doc_rev_other(self):
        self.assertEqual('machine:1|test:1',
                         self.c._allocate_doc_rev('machine:1'))

    def test__get_machine_id(self):
        self.assertEqual('test', self.c._machine_id)

    def test_put_doc_creating_initial(self):
        c = client.InMemoryClient()
        doc = '{"doc": "value"}'
        self.c.put_doc('my_doc_id', None, doc)
        self.assertEqual({'my_doc_id': (None, doc)},
                         self.c._docs)

    def test_get_doc_after_put(self):
        doc = '{"doc": "value"}'
        self.c.put_doc('my_doc_id', None, doc)
        self.assertEqual((None, doc, False), self.c.get_doc('my_doc_id'))
