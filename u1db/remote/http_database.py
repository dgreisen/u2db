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


from u1db import (
    Database
    )
from u1db.remote import (
    http_client,
    )


class HTTPDatabase(http_client.HTTPClientBase, Database):
    """Implement the Database API to a remote HTTP server."""

    def put_doc(self, doc_id, old_doc_rev, doc):
        params = {}
        if old_doc_rev is not None:
            params['old_rev'] = old_doc_rev
        res, headers = self._request_json('PUT', ['doc', doc_id], params,
                                          doc, 'application/json')
        return res['rev']


    def get_doc(self, doc_id):
        res, headers = self._request('GET', ['doc', doc_id])
        doc_rev = headers['x-u1db-rev']
        has_conflicts = simplejson.loads(headers['x-u1db-has-conflicts'])
        return doc_rev, res, has_conflicts
