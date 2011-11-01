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

import u1db
from u1db.vectorclock import VectorClockRev


class CommonSyncTarget(u1db.SyncTarget):

    def __init__(self, db):
        self._db = db
        self.conflict_ids = set()
        self.seen_ids = set() # not superseded
        self.my_gen = None
        self.changed_doc_ids = None
        self._docs_trace = [] # for tests

    def _insert_other_doc(self, doc_id, doc_rev, doc):
        """Try to insert synced over document.

        :return: None
        """
        state = self._db.put_doc_if_newer(doc_id, doc_rev, doc)
        if state == 'inserted':
            self.seen_ids.add(doc_id)
        elif state == 'converged':
            # magical convergence
            self.seen_ids.add(doc_id)
        elif state == 'superseded':
            pass
        else:
            assert state == 'conflicted'
            self.seen_ids.add(doc_id)
            self.conflict_ids.add(doc_id)
        # for tests
        self._docs_trace.append((doc_id, doc_rev))

    def sync_exchange(self, docs_info,
                      from_replica_uid, from_replica_generation,
                      last_known_generation, return_doc_cb):
        for doc_id, doc_rev, doc in docs_info:
            self._insert_other_doc(doc_id, doc_rev, doc)
        my_gen = self._checkpoint_sync_exchange(from_replica_uid,
                                                from_replica_generation,
                                                last_known_generation)
        self._finish_sync_exchange(from_replica_uid,
                                   from_replica_generation,
                                   last_known_generation, return_doc_cb)
        return my_gen

    def _checkpoint_sync_exchange(self, from_replica_uid,
                                  from_replica_generation,
                                  last_known_generation):
        my_gen, changed_doc_ids = self._db.whats_changed(last_known_generation)
        self.my_gen = my_gen
        self.changed_doc_ids = changed_doc_ids
        return my_gen

    def _finish_sync_exchange(self, from_replica_uid, from_replica_generation,
                         last_known_generation, return_doc_cb):
        seen_ids = self.seen_ids
        conflict_ids = self.conflict_ids
        my_gen = self.my_gen
        changed_doc_ids = self.changed_doc_ids
        doc_ids_to_return = [doc_id for doc_id in changed_doc_ids
                             if doc_id not in seen_ids]
        new_docs = self._db.get_docs(doc_ids_to_return,
                                     check_for_conflicts=False)
        for doc_id, doc_rev, doc, _ in new_docs:
            return_doc_cb(doc_id, doc_rev, doc)
        conflicts = self._db.get_docs(conflict_ids, check_for_conflicts=False)
        for doc_id, doc_rev, doc, _ in conflicts:
            return_doc_cb(doc_id, doc_rev, doc)
        self._db.set_sync_generation(from_replica_uid,
                                     from_replica_generation)
        self._db._last_exchange_log = {
            'receive': {'docs': self._docs_trace,
                        'from_id': from_replica_uid,
                        'from_gen': from_replica_generation,
                        'last_known_gen': last_known_generation},
            'return': {'new_docs': [(di, dr) for di, dr, _, _ in new_docs],
                       'conf_docs': [(di, dr) for di, dr, _, _ in conflicts],
                       'last_gen': my_gen}
        }
        return my_gen


class CommonBackend(u1db.Database):

    def _allocate_doc_id(self):
        """Generate a unique identifier for this document."""
        raise NotImplementedError(self._allocate_doc_id)

    def _allocate_doc_rev(self, old_doc_rev):
        vcr = VectorClockRev(old_doc_rev)
        vcr.increment(self._replica_uid)
        return vcr.as_str()

    def _get_generation(self):
        raise NotImplementedError(self._get_generation)

    def _get_doc(self, doc_id):
        """Extract the document from storage.

        This can return None if the document doesn't exist, it should not check
        if there are any conflicts, etc.
        """
        raise NotImplementedError(self._get_doc)

    def _has_conflicts(self, doc_id):
        """Return True if the doc has conflicts, False otherwise."""
        raise NotImplementedError(self._has_conflicts)

    def create_doc(self, doc, doc_id=None):
        if doc_id is None:
            doc_id = self._allocate_doc_id()
        return doc_id, self.put_doc(doc_id, None, doc)

    def _get_transaction_log(self):
        """This is only for the test suite, it is not part of the api."""
        raise NotImplementedError(self._get_transaction_log)

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        raise NotImplementedError(self._put_and_update_indexes)

    def _compare_and_insert_doc(self, doc_id, doc_rev, doc):
        """Check if a document is newer than current and insert it.

        :return: (old_doc, state)
        """
        cur_rev, cur_doc = self._get_doc(doc_id)
        doc_vcr = VectorClockRev(doc_rev)
        cur_vcr = VectorClockRev(cur_rev)
        if doc_vcr.is_newer(cur_vcr):
            self._put_and_update_indexes(doc_id, cur_doc, doc_rev, doc)
            return cur_doc, 'inserted'
        elif doc_rev == cur_rev:
            # magical convergence
            return cur_doc, 'converged'
        elif cur_vcr.is_newer(doc_vcr):
            # Don't add this to seen_ids, because we have something newer,
            # so we should send it back, and we should not generate a
            # conflict
            return cur_doc, 'superseded'
        else:
            return cur_doc, 'conflicted'

    def get_docs(self, doc_ids, check_for_conflicts=True):
        if check_for_conflicts:
            result = []
            for doc_id in doc_ids:
                doc_rev, doc = self._get_doc(doc_id)
                is_conflicted = self._has_conflicts(doc_id)
                result.append((doc_id, doc_rev, doc, is_conflicted))
        else:
            result = [(doc_id,) + self._get_doc(doc_id) + (None,)
                      for doc_id in doc_ids]
        return result

    def put_doc_if_newer(self, doc_id, doc_rev, doc):
        _, state = self._compare_and_insert_doc(doc_id, doc_rev, doc)
        return state

    def _ensure_maximal_rev(self, cur_rev, extra_revs):
        vcr = VectorClockRev(cur_rev)
        for rev in extra_revs:
            vcr.maximize(VectorClockRev(rev))
        vcr.increment(self._replica_uid)
        return vcr.as_str()
