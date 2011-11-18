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

"""Tests for HTTPDatabase"""

import inspect
import os
import simplejson
from wsgiref import simple_server

from u1db import (
    Document,
    tests,
    )
from u1db.remote import (
    http_database
    )


class TestHTTPDatabaseSimpleOperations(tests.TestCase):

    def setUp(self):
        super(TestHTTPDatabaseSimpleOperations, self).setUp()
        self.db = http_database.HTTPDatabase('dbase')
        self.db._conn = object() # crash if used
        self.got = None
        self.response_val = None
        def _request(method, url_parts, params=None, body=None,
                                                     content_type=None):
            self.got = method, url_parts, params, body, content_type
            return self.response_val
        def _request_json(method, url_parts, params=None, body=None,
                                                          content_type=None):
            self.got = method, url_parts, params, body, content_type
            return self.response_val
        self.db._request = _request
        self.db._request_json = _request_json

    def test__sanity_same_signature(self):
        my_request_sig = inspect.getargspec(self.db._request)
        my_request_sig = (['self'] + my_request_sig[0],) + my_request_sig[1:]
        self.assertEqual(my_request_sig,
                       inspect.getargspec(http_database.HTTPDatabase._request))
        my_request_json_sig = inspect.getargspec(self.db._request_json)
        my_request_json_sig = ((['self'] + my_request_json_sig[0],) +
                               my_request_json_sig[1:])
        self.assertEqual(my_request_json_sig,
                  inspect.getargspec(http_database.HTTPDatabase._request_json))

    def test_put_doc(self):
        self.response_val = {'rev': 'doc-rev'}, {}
        doc = Document('doc-id', None, '{"v": 1}')
        res = self.db.put_doc(doc)
        self.assertEqual('doc-rev', res)
        self.assertEqual('doc-rev', doc.rev)
        self.assertEqual(('PUT', ['doc', 'doc-id'], {},
                          '{"v": 1}', 'application/json'), self.got)

        self.response_val = {'rev': 'doc-rev-2'}, {}
        doc.set_content('{"v": 2}')
        res = self.db.put_doc(doc)
        self.assertEqual('doc-rev-2', res)
        self.assertEqual('doc-rev-2', doc.rev)
        self.assertEqual(('PUT', ['doc', 'doc-id'], {'old_rev': 'doc-rev'},
                          '{"v": 2}', 'application/json'), self.got)

    def test_get_doc(self):
        self.response_val = '{"v": 2}', {'x-u1db-rev': 'doc-rev',
                                         'x-u1db-has-conflicts': 'false'}
        self.assertGetDoc(self.db, 'doc-id', 'doc-rev', '{"v": 2}', False)
        self.assertEqual(('GET', ['doc', 'doc-id'], None, None, None),
                         self.got)
