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

"""HTTPDabase to access a remote db over the HTTP API."""

import simplejson
import uuid

from u1db import (
    Database,
    Document,
    errors,
    )
from u1db.remote import (
    http_client,
    )


class HTTPDatabase(http_client.HTTPClientBase, Database):
    """Implement the Database API to a remote HTTP server."""

    def put_doc(self, doc):
        if doc.doc_id is None:
            raise errors.InvalidDocId()
        params = {}
        if doc.rev is not None:
            params['old_rev'] = doc.rev
        res, headers = self._request_json('PUT', ['doc', doc.doc_id], params,
                                          doc.content, 'application/json')
        doc.rev = res['rev']
        return res['rev']

    def get_doc(self, doc_id):
        try:
            res, headers = self._request('GET', ['doc', doc_id])
        except errors.HTTPError, e:
            if e.status == 404:
                if e.headers.get('x-u1db-rev') == 'null':
                    return None
            raise
        doc_rev = headers['x-u1db-rev']
        has_conflicts = simplejson.loads(headers['x-u1db-has-conflicts'])
        doc = Document(doc_id, doc_rev, res)
        doc.has_conflicts = has_conflicts
        return doc

    def create_doc(self, content, doc_id=None):
        if doc_id is None:
            doc_id = str(uuid.uuid4())
        res, headers = self._request_json('PUT', ['doc', doc_id], {},
                                          content, 'application/json')
        new_doc = Document(doc_id, res['rev'], content)
        return new_doc
