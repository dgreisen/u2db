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

""""""

import json

from u1db import (
    SyncTarget,
    )
from u1db.remote import (
    http_client,
    )


class HTTPSyncTarget(http_client.HTTPClientBase, SyncTarget):
    """Implement the SyncTarget api to a remote HTTP server."""

    @staticmethod
    def connect(url):
        return HTTPSyncTarget(url)

    def get_sync_info(self, other_replica_uid):
        self._ensure_connection()
        self._conn.request('GET', '%s/sync-from/%s' % (self._url.path,
                                                         other_replica_uid))
        # xxx check for errors with status
        res = json.loads(self._conn.getresponse().read())
        return (res['this_replica_uid'], res['this_replica_generation'],
                res['other_replica_generation'])

    def record_sync_info(self, other_replica_uid, other_replica_generation):
        self._ensure_connection()
        self._conn.request('PUT',
                          '%s/sync-from/%s' % (self._url.path,
                                               other_replica_uid),
                          json.dumps({'generation': other_replica_generation}),
                          {'content-type': 'application/json'})
        self._conn.getresponse().read() # xxx check for errors with status

    def sync_exchange(self, docs_info, from_replica_uid,
                      from_replica_generation,
                      last_known_generation, return_doc_cb):
        self._ensure_connection()
        self._conn.putrequest('POST',
                                '%s/sync-from/%s' % (self._url.path,
                                                     from_replica_uid))
        self._conn.putheader('content-type', 'application/x-u1db-multi-json')
        entries = []
        size = 0
        def prepare(**dic):
            entry = json.dumps(dic)+"\r\n"
            entries.append(entry)
            return len(entry)
        size += prepare(last_known_generation=last_known_generation,
                        from_replica_generation=from_replica_generation)
        for doc_id, doc_rev, doc in docs_info:
            size += prepare(id=doc_id, rev=doc_rev, doc=doc)
        self._conn.putheader('content-length', str(size))
        self._conn.endheaders()
        for entry in entries:
            self._conn.send(entry)
        entries = None
        resp = self._conn.getresponse() # xxx check for errors with status
        data = resp.read().splitlines() # one at a time
        res = json.loads(data[0])
        for entry in data[1:]:
            entry = json.loads(entry)
            return_doc_cb(entry['id'], entry['rev'], entry['doc'])
        data = None
        return res['new_generation']

    def get_sync_exchange(self):
        return None # not a local target
