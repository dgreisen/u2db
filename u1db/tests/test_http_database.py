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
import simplejson

from u1db import (
    errors,
    Document,
    tests,
    )
from u1db.remote import (
    http_database
    )
from u1db.tests.test_remote_sync_target import (
    http_server_def,
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
            if isinstance(self.response_val, Exception):
                raise self.response_val
            return self.response_val
        def _request_json(method, url_parts, params=None, body=None,
                                                          content_type=None):
            self.got = method, url_parts, params, body, content_type
            if isinstance(self.response_val, Exception):
                raise self.response_val
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
        doc.content = '{"v": 2}'
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

    def test_get_doc_non_existing(self):
        self.response_val = errors.DocumentDoesNotExist()
        self.assertIs(None, self.db.get_doc('not-there'))
        self.assertEqual(('GET', ['doc', 'not-there'], None, None, None),
                         self.got)

    def test_get_doc_deleted(self):
        self.response_val = errors.HTTPError(404,
                                             simplejson.dumps(
                                             {"error": errors.DOCUMENT_DELETED}
                                             ),
                                             {'x-u1db-rev': 'doc-rev-gone',
                                              'x-u1db-has-conflicts': 'false'})
        doc = self.db.get_doc('deleted')
        self.assertEqual('deleted', doc.doc_id)
        self.assertEqual('doc-rev-gone', doc.rev)
        self.assertIs(None, doc.content)

    def test_get_doc_pass_through_errors(self):
        self.response_val = errors.HTTPError(500, 'Crash.')
        self.assertRaises(errors.HTTPError,
                          self.db.get_doc, 'something-something')

    def test_create_doc_with_id(self):
        self.response_val = {'rev': 'doc-rev'}, {}
        new_doc = self.db.create_doc('{"v": 1}', doc_id='doc-id')
        self.assertEqual('doc-rev', new_doc.rev)
        self.assertEqual('doc-id', new_doc.doc_id)
        self.assertEqual('{"v": 1}', new_doc.content)
        self.assertEqual(('PUT', ['doc', 'doc-id'], {},
                          '{"v": 1}', 'application/json'), self.got)

    def test_create_doc_without_id(self):
        self.response_val = {'rev': 'doc-rev-2'}, {}
        new_doc = self.db.create_doc('{"v": 3}')
        self.assertEqual('doc-rev-2', new_doc.rev)
        self.assertEqual('{"v": 3}', new_doc.content)
        self.assertEqual(('PUT', ['doc', new_doc.doc_id], {},
                          '{"v": 3}', 'application/json'), self.got)

    def test_delete_doc(self):
        self.response_val = {'rev': 'doc-rev-gone'}, {}
        doc = Document('doc-id', 'doc-rev', None)
        self.db.delete_doc(doc)
        self.assertEqual('doc-rev-gone', doc.rev)
        self.assertEqual(('DELETE', ['doc', 'doc-id'], {'old_rev': 'doc-rev'},
                          None, None), self.got)


class TestHTTPDatabaseIntegration(tests.TestCaseWithServer):

    server_def = staticmethod(http_server_def)

    def test_non_existing_db(self):
        self.startServer()
        db = http_database.HTTPDatabase(self.getURL('not-there'))
        self.assertRaises(errors.DatabaseDoesNotExist, db.get_doc, 'doc1')
