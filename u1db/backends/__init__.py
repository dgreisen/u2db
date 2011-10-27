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

    def sync_exchange(self, docs_info,
                      from_machine_id, from_machine_generation,
                      last_known_generation):
        (conflict_ids, superseded_ids,
         num_inserted) = self._db.put_docs_if_newer(docs_info)
        seen_ids = [x[0] for x in docs_info if x[0] not in superseded_ids]
        new_docs = []
        my_gen, changed_doc_ids = self._db.whats_changed(last_known_generation)
        doc_ids_to_return = [doc_id for doc_id in changed_doc_ids
                             if doc_id not in seen_ids]
        new_docs = self._db.get_docs(doc_ids_to_return,
                                     check_for_conflicts=False)
        new_docs = [x[:3] for x in new_docs]
        conflicts = self._db.get_docs(conflict_ids, check_for_conflicts=False)
        conflicts = [x[:3] for x in conflicts]
        self._db.set_sync_generation(from_machine_id,
                                     from_machine_generation)
        self._db._last_exchange_log = {
            'receive': {'docs': [(di, dr) for di, dr, _ in docs_info],
                        'from_id': from_machine_id,
                        'from_gen': from_machine_generation,
                        'last_known_gen': last_known_generation},
            'return': {'new_docs': [(di, dr) for di, dr, _ in new_docs],
                       'conf_docs': [(di, dr) for di, dr, _ in conflicts],
                       'last_gen': my_gen}
        }
        return new_docs, conflicts, my_gen



class CommonBackend(u1db.Database):

    def _allocate_doc_id(self):
        """Generate a unique identifier for this document."""
        raise NotImplementedError(self._allocate_doc_id)

    def _allocate_doc_rev(self, old_doc_rev):
        vcr = VectorClockRev(old_doc_rev)
        vcr.increment(self._machine_id)
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

    def put_docs_if_newer(self, docs_info):
        superseded_ids = set()
        conflict_ids = set()
        num_inserted = 0
        for doc_id, doc_rev, doc in docs_info:
            old_doc, state = self._compare_and_insert_doc(doc_id, doc_rev, doc)
            if state == 'inserted':
                num_inserted += 1
            elif state == 'converged':
                # magical convergence
                continue
            elif state == 'superseded':
                superseded_ids.add(doc_id)
                continue
            else:
                assert state == 'conflicted'
                conflict_ids.add(doc_id)
        return conflict_ids, superseded_ids, num_inserted

    def _ensure_maximal_rev(self, cur_rev, extra_revs):
        vcr = VectorClockRev(cur_rev)
        for rev in extra_revs:
            vcr.maximize(VectorClockRev(rev))
        vcr.increment(self._machine_id)
        return vcr.as_str()
