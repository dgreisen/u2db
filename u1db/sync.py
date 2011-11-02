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

"""The synchronization utilities for U1DB."""

import u1db

class Synchronizer(object):
    """Collect the state around synchronizing 2 U1DB replicas.

    Synchronization is bi-directional, in that new items in the source are sent
    to the target, and new items in the target are returned to the source.
    However, it still recognizes that one side is initiating the request. Also,
    at the moment, conflicts are only created in the source.
    """

    def __init__(self, source, sync_target):
        """Create a new Synchronization object.

        :param source: A Database
        :param sync_target: A SyncTarget
        """
        self.source = source
        self.sync_target = sync_target
        self.num_inserted = 0

    def _insert_doc_from_target(self, doc_id, doc_rev, doc):
        """Try to insert synced document from target.

        Implements TAKE OTHER semantics: any document from the target
        that is in conflict will be taken as the new official value,
        while the current conflicting value will be stored alongside
        as a conflict. In the process indexes will be updated etc.

        :return: None
        """
        # Increases self.num_inserted depending whether the document
        # was effectively inserted.
        state = self.source.put_doc_if_newer(doc_id, doc_rev, doc)
        if state == 'inserted':
            self.num_inserted += 1
        elif state == 'converged':
            # magical convergence
            pass
        elif state == 'superseded':
            pass
        else:
            assert state == 'conflicted'
            self.source.force_doc_sync_conflict(doc_id, doc_rev, doc)
            self.num_inserted += 1

    def sync(self, callback=None):
        sync_target = self.sync_target
        (other_replica_uid, other_gen,
         others_my_gen) = sync_target.get_sync_info(self.source._replica_uid)
        my_gen, changed_doc_ids = self.source.whats_changed(others_my_gen)
        docs_to_send = self.source.get_docs(changed_doc_ids,
            check_for_conflicts=False)
        docs_to_send = [x[:3] for x in docs_to_send]
        other_last_known_gen = self.source.get_sync_generation(other_replica_uid)
        new_gen = sync_target.sync_exchange(docs_to_send,
                        self.source._replica_uid, my_gen, other_last_known_gen,
                        return_doc_cb=self._insert_doc_from_target)
        self.source.set_sync_generation(other_replica_uid, new_gen)
        cur_gen = self.source._get_generation()
        if cur_gen == my_gen + self.num_inserted:
            sync_target.record_sync_info(self.source._replica_uid, cur_gen)
        return my_gen


class SyncExchange(object):
    """Steps and state for carrying through a sync exchange on a target."""

    def __init__(self, db):
        self._db = db
        self.seen_ids = set() # incoming ids not superseded (or conflicted)
        self.doc_ids_to_return = None
        self.conflict_ids = set()
        self.new_gen = None
        # for tests
        self._incoming_trace = []
        self._last_known_generation = None

    def insert_doc_from_source(self, doc_id, doc_rev, doc):
        """Try to insert synced document from source.

        Conflicting documents are not inserted but the current revision
        marked to be sent over to the sync source.

        The 1st step of a sync exchange is to call this repeatedly to
        try insert all incoming documents from the source.

        :param doc_id: The unique handle for a document.
        :param doc_rev: The document revision to try to store.
        :param doc: The actual JSON document string.
        :return: None
        """
        state = self._db.put_doc_if_newer(doc_id, doc_rev, doc)
        if state == 'inserted':
            self.seen_ids.add(doc_id)
        elif state == 'converged':
            # magical convergence
            self.seen_ids.add(doc_id)
        elif state == 'superseded':
            # we have something newer that we will return
            pass
        else:
            # conflict, returned independently
            assert state == 'conflicted'
            self.seen_ids.add(doc_id)
            self.conflict_ids.add(doc_id)
        # for tests
        self._incoming_trace.append((doc_id, doc_rev))

    def find_docs_to_return(self, last_known_generation):
        """Find and mark all documents to return to the sync source,
        either because they changed since the last known generation
        the source has for the target, including ones that were sent
        over but there is conflict.

        The is the 2nd step of a sync exchange.

        :param last_known_generation: The last generation that the source
            knows about this
        :return:  new_generation -  The current generation for the target
            considering also all the inserted incoming documents.
        """
        self._last_known_generation = last_known_generation # for tests
        new_gen, changed_doc_ids = self._db.whats_changed(last_known_generation)
        self.new_gen = new_gen
        seen_ids = self.seen_ids
        # changed docs that weren't superseded by or converged with
        # nor conflicted, conflicts are returned independently
        self.doc_ids_to_return = set(doc_id for doc_id in changed_doc_ids
                                     if doc_id not in seen_ids)
        return new_gen

    def return_docs_and_record_sync(self,
                                    from_replica_uid, from_replica_generation,
                                    return_doc_cb):
        """Return the marked documents repeatedly invoking the callback
        return_doc_cb, record the sync information of from_replica_uid
        the sync source identifier and the generation until which it
        sent its documents from_replica_generation.

        The final step of a sync exchange.

        :param from_replica_uid: The source replica's identifier
        :param from_replica_generation: The db generation for the
            source replica indicating the tip of data that was sent.
        :param: return_doc_cb(doc_id, doc_rev, doc): is a callback
                used to return the marked documents to the target replica,
        :return: None
        """
        doc_ids_to_return = self.doc_ids_to_return
        conflict_ids = self.conflict_ids
        # return docs
        new_docs = self._db.get_docs(doc_ids_to_return,
                                     check_for_conflicts=False)
        for doc_id, doc_rev, doc, _ in new_docs:
            return_doc_cb(doc_id, doc_rev, doc)
        conflicts = self._db.get_docs(conflict_ids, check_for_conflicts=False)
        for doc_id, doc_rev, doc, _ in conflicts:
            return_doc_cb(doc_id, doc_rev, doc)
        # record sync point
        self._db.set_sync_generation(from_replica_uid,
                                     from_replica_generation)
        # for tests
        self._db._last_exchange_log = {
            'receive': {'docs': self._incoming_trace,
                        'from_id': from_replica_uid,
                        'from_gen': from_replica_generation,
                        'last_known_gen': self._last_known_generation},
            'return': {'new_docs': [(di, dr) for di, dr, _, _ in new_docs],
                       'conf_docs': [(di, dr) for di, dr, _, _ in conflicts],
                       'last_gen': self.new_gen}
        }


class LocalSyncTarget(u1db.SyncTarget):
    """Common sync target implementation logic for all local sync targets."""

    def __init__(self, db):
        self._db = db

    def get_sync_exchange(self):
        return SyncExchange(self._db)

    def sync_exchange(self, docs_info,
                      from_replica_uid, from_replica_generation,
                      last_known_generation, return_doc_cb):
        sync_exch = self.get_sync_exchange()
        # 1st step: try to insert incoming docs
        for doc_id, doc_rev, doc in docs_info:
            sync_exch.insert_doc_from_source(doc_id, doc_rev, doc)
        # 2nd step: find changed documents (including conflicts) to return
        new_gen = sync_exch.find_docs_to_return(last_known_generation)
        # final step: return docs and record source replica sync point
        sync_exch.return_docs_and_record_sync(from_replica_uid,
                                              from_replica_generation,
                                              return_doc_cb)
        return new_gen
