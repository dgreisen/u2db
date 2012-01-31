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

"""SyncTarget API implementation to a remote HTTP server."""

import simplejson

from u1db import (
    Document,
    errors,
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

    def get_sync_info(self, source_replica_uid):
        self._ensure_connection()
        res, _ = self._request_json('GET', ['sync-from', source_replica_uid])
        return (res['target_replica_uid'], res['target_replica_generation'],
                res['source_replica_generation'])

    def record_sync_info(self, source_replica_uid, source_replica_generation):
        self._ensure_connection()
        self._request_json('PUT', ['sync-from', source_replica_uid], {},
                                  {'generation': source_replica_generation})

    def sync_exchange(self, docs_by_generations, source_replica_uid,
                      last_known_generation, return_doc_cb):
        self._ensure_connection()
        self._conn.putrequest('POST',
                                '%s/sync-from/%s' % (self._url.path,
                                                     source_replica_uid))
        self._conn.putheader('content-type', 'application/x-u1db-multi-json')
        entries = []
        size = 0
        def prepare(**dic):
            entry = simplejson.dumps(dic) + "\r\n"
            entries.append(entry)
            return len(entry)
        size += prepare(last_known_generation=last_known_generation)
        for doc, gen in docs_by_generations:
            size += prepare(id=doc.doc_id, rev=doc.rev, content=doc.content,
                            gen=gen)
        self._conn.putheader('content-length', str(size))
        self._conn.endheaders()
        for entry in entries:
            self._conn.send(entry)
        entries = None
        data, _ = self._response()
        parts = data.splitlines()  # one at a time
        if parts[0] != '[':
            raise errors.BrokenSyncStream("expected [ on first stream line")
        if parts[-1] != ']':
            raise errors.BrokenSyncStream("expected ] on last stream line")
        data = parts[1:-1]
        res = simplejson.loads(data[0].rstrip(","))
        for entry in data[1:]:
            entry = simplejson.loads(entry.rstrip(","))
            doc = Document(entry['id'], entry['rev'], entry['content'])
            return_doc_cb(doc, entry['gen'])
        data = None
        return res['new_generation']

    def get_sync_exchange(self, source_replica_uid):
        return None  # not a local target
