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

    def _insert_doc_from_target(self, doc):
        """Try to insert synced document from target.

        Implements TAKE OTHER semantics: any document from the target
        that is in conflict will be taken as the new official value,
        while the current conflicting value will be stored alongside
        as a conflict. In the process indexes will be updated etc.

        :return: None
        """
        # Increases self.num_inserted depending whether the document
        # was effectively inserted.
        state = self.source.put_doc_if_newer(doc)
        if state == 'inserted':
            self.num_inserted += 1
        elif state == 'converged':
            # magical convergence
            pass
        elif state == 'superseded':
            # we have something newer, will be taken care of at the next sync
            pass
        else:
            assert state == 'conflicted'
            # take doc as the official value, stores the current
            # alongside as conflict
            self.source.force_doc_sync_conflict(doc)
            self.num_inserted += 1

    def _record_sync_info_with_the_target(self, start_generation):
        """Record our new after sync generation with the target if gapless.

        Any documents received from the target will cause the local
        database to increment its generation. We do not want to send
        them back to the target in a future sync. However, there could
        also be concurrent updates from another process doing eg
        'put_doc' while the sync was running. And we do want to
        synchronize those documents.  We can tell if there was a
        concurrent update by comparing our new generation number
        versus the generation we started, and how many documents we
        inserted from the target. If it matches exactly, then we can
        record with the target that they are fully up to date with our
        new generation.
        """
        cur_gen = self.source._get_generation()
        if cur_gen == start_generation + self.num_inserted:
            self.sync_target.record_sync_info(self.source._replica_uid,
                                              cur_gen)

    def sync(self, callback=None):
        """Synchronize documents between source and target."""
        sync_target = self.sync_target
        # get target identifier, its current generation,
        # and its last-seen database generation for this source
        (other_replica_uid, other_gen,
         others_my_gen) = sync_target.get_sync_info(self.source._replica_uid)
        # what's changed since that generation and this current gen
        my_gen, changes = self.source.whats_changed(others_my_gen)
        changed_doc_ids = set(doc_id for doc_id, _ in changes)
        # prepare to send all the changed docs
        docs_to_send = self.source.get_docs(changed_doc_ids,
            check_for_conflicts=False)

        # this source last-seen database generation for the target
        other_last_known_gen = self.source.get_sync_generation(
            other_replica_uid)
        # exchange documents and try to insert the returned ones with
        # the target, return target synced-up-to gen
        new_gen = sync_target.sync_exchange(docs_to_send,
                        self.source._replica_uid, my_gen, other_last_known_gen,
                        return_doc_cb=self._insert_doc_from_target)
        # record target synced-up-to generation
        self.source.set_sync_generation(other_replica_uid, new_gen)

        # if gapless record current reached generation with target
        self._record_sync_info_with_the_target(my_gen)

        return my_gen


class SyncExchange(object):
    """Steps and state for carrying through a sync exchange on a target."""

    def __init__(self, db):
        self._db = db
        self.seen_ids = set()  # incoming ids not superseded
        self.doc_ids_to_return = None
        self.new_gen = None
        # for tests
        self._incoming_trace = []
        self._db._last_exchange_log = {
            'receive': {'docs': self._incoming_trace},
            'return': None
            }

    def insert_doc_from_source(self, doc):
        """Try to insert synced document from source.

        Conflicting documents are not inserted but will be sent over
        to the sync source.

        The 1st step of a sync exchange is to call this repeatedly to
        try insert all incoming documents from the source.

        :param doc: A Document object.
        :return: None
        """
        state = self._db.put_doc_if_newer(doc)
        if state == 'inserted':
            self.seen_ids.add(doc.doc_id)
        elif state == 'converged':
            # magical convergence
            self.seen_ids.add(doc.doc_id)
        elif state == 'superseded':
            # we have something newer that we will return
            pass
        else:
            # conflict that we will returne
            assert state == 'conflicted'
        # for tests
        self._incoming_trace.append((doc.doc_id, doc.rev))

    def record_sync_progress(self, from_replica_uid, from_replica_generation):
        """Record the sync information of from_replica_uid
        the sync source identifier and the generation until which it
        sent its documents from_replica_generation.

        :param from_replica_uid: The source replica's identifier
        :param from_replica_generation: The db generation for the
            source replica indicating the tip of data that was sent.
        :return: None
        """
        # record sync point
        self._db.set_sync_generation(from_replica_uid,
                                     from_replica_generation)
        # for tests
        self._db._last_exchange_log['receive'].update({
            'from_id': from_replica_uid,
            'from_gen': from_replica_generation
            })

    def find_docs_to_return(self, last_known_generation):
        """Find and further mark documents to return to the sync source.

        This finds the document identifiers for any documents that
        have been updated since last_known_generation. It excludes
        documents ids that have already been considered
        (superseded by the sender, etc).

        :return: new_generation - the generation of this database
            which the caller can consider themselves to be synchronized after
            processing the returned documents.
        """
        self._db._last_exchange_log['receive'].update({  # for tests
            'last_known_gen': last_known_generation
            })
        gen, changes = self._db.whats_changed(last_known_generation)
        changed_doc_ids = set(doc_id for doc_id, _ in changes)
        self.new_gen = gen
        seen_ids = self.seen_ids
        # changed docs that weren't superseded by or converged with
        self.doc_ids_to_return = set(doc_id for doc_id in changed_doc_ids
                                     if doc_id not in seen_ids)
        return gen

    def return_docs(self, return_doc_cb):
        """Return the marked documents repeatedly invoking the callback
        return_doc_cb.

        The final step of a sync exchange.

        :param: return_doc_cb(doc): is a callback
                used to return the marked documents to the target replica,
        :return: None
        """
        doc_ids_to_return = self.doc_ids_to_return
        # return docs, including conflicts
        docs = self._db.get_docs(doc_ids_to_return,
                                     check_for_conflicts=False)
        for doc in docs:
            return_doc_cb(doc)
        # for tests
        self._db._last_exchange_log['return'] = {
            'docs': [(d.doc_id, d.rev) for d in docs],
            'last_gen': self.new_gen
            }


class LocalSyncTarget(u1db.SyncTarget):
    """Common sync target implementation logic for all local sync targets."""

    def __init__(self, db):
        self._db = db

    def get_sync_exchange(self):
        return SyncExchange(self._db)

    def sync_exchange(self, docs,
                      from_replica_uid, from_replica_generation,
                      last_known_generation, return_doc_cb):
        sync_exch = self.get_sync_exchange()
        # 1st step: try to insert incoming docs
        for doc in docs:
            sync_exch.insert_doc_from_source(doc)
        # record progress
        sync_exch.record_sync_progress(from_replica_uid,
                                       from_replica_generation)
        # 2nd step: find changed documents (including conflicts) to return
        new_gen = sync_exch.find_docs_to_return(last_known_generation)
        # final step: return docs and record source replica sync point
        sync_exch.return_docs(return_doc_cb)
        return new_gen
