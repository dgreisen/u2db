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

import socket
import urlparse

from u1db import (
    SyncTarget,
    )
from u1db.remote import (
    client,
    )

class _DocSource(client.EntrySource):

    @staticmethod
    def prepare((doc_id, doc_rev, doc)):
        return dict(doc_id=doc_id, doc_rev=doc_rev, doc=doc)

class RemoteSyncTarget(SyncTarget):
    """Implement the SyncTarget api to a remote server."""

    @staticmethod
    def connect(url):
        return RemoteSyncTarget(url)

    def __init__(self, url):
        self._url = urlparse.urlsplit(url)
        self._conn = None
        self._client = None

    def _ensure_connection(self):
        if self._conn is not None:
            return
        self._conn = socket.socket()
        self._conn.connect((self._url.hostname, self._url.port))
        self._client = client.Client(self._conn)

    def get_sync_info(self, other_replica_uid):
        self._ensure_connection()
        res = self._client.call_returning_args("get_sync_info",
            path=self._url.path, other_replica_uid=other_replica_uid)
        return (res['this_replica_uid'], res['this_replica_generation'],
                res['other_replica_generation'])

    def record_sync_info(self, other_replica_uid, other_replica_generation):
        self._ensure_connection()
        self._client.call_returning_args("record_sync_info",
                             path=self._url.path,
                             other_replica_uid=other_replica_uid,
                             other_replica_generation=other_replica_generation)

    def sync_exchange(self, docs_info, from_replica_uid,
                      from_replica_generation,
                      last_known_generation, return_doc_cb):
        self._ensure_connection()
        # xxx docs_info itself should be replaced by a callback one level up
        doc_source = _DocSource(docs_info)
        def take_entry(entry):
            return_doc_cb(entry['doc_id'], entry['doc_rev'], entry['doc'])
        res = self._client.call_with_streaming("sync_exchange",
                               doc_source.cb,
                               take_entry,
                               path=self._url.path,
                               from_replica_uid=from_replica_uid,
                               from_replica_generation=from_replica_generation,
                               last_known_generation=last_known_generation)
        return res['other_new_generation']

    def get_sync_exchange(self):
        return None # not a local target
