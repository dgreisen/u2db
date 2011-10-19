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

    def sync_exchange(self, docs_info, from_machine_id, from_machine_rev,
                      last_known_rev):
        (conflict_ids, superseded_ids,
         num_inserted) = self._db.put_docs(docs_info)
        seen_ids = [x[0] for x in docs_info if x[0] not in superseded_ids]
        new_docs = []
        my_db_rev, changed_doc_ids = self._db.whats_changed(last_known_rev)
        doc_ids_to_return = [doc_id for doc_id in changed_doc_ids
                             if doc_id not in seen_ids]
        new_docs = self._db.get_docs(doc_ids_to_return)
        conflicts = self._db.get_docs(conflict_ids)
        self._db.set_sync_generation(from_machine_id, from_machine_rev)
        self._db._last_exchange_log = {
            'receive': {'docs': [(di, dr) for di, dr, _ in docs_info],
                        'from_id': from_machine_id,
                        'from_rev': from_machine_rev,
                        'last_known_rev': last_known_rev},
            'return': {'new_docs': [(di, dr) for di, dr, _ in new_docs],
                       'conf_docs': [(di, dr) for di, dr, _ in conflicts],
                       'last_rev': my_db_rev}
        }
        return new_docs, conflicts, my_db_rev



class CommonBackend(u1db.Database):

    def _allocate_doc_id(self):
        """Generate a unique identifier for this document."""
        raise NotImplementedError(self._allocate_doc_id)

    def _allocate_doc_rev(self, old_doc_rev):
        vcr = VectorClockRev(old_doc_rev)
        vcr.increment(self._machine_id)
        return vcr.as_str()

    def _get_db_rev(self):
        raise NotImplementedError(self._get_db_rev)

    def _get_doc(self, doc_id):
        """Extract the document from storage.

        This can return None if the document doesn't exist, it should not check
        if there are any conflicts, etc.
        """
        raise NotImplementedError(self._get_doc)

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

    def get_docs(self, doc_ids):
        return [(doc_id,) + self._get_doc(doc_id) for doc_id in doc_ids]

    def put_docs(self, docs_info):
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

    def _insert_conflicts(self, docs_info):
        """Record all of docs_info as conflicted documents.

        Because of the 'TAKE_OTHER' semantics, any document which is marked as
        conflicted takes docs_info as the official value.
        This will update index definitions, etc.

        :return: The number of documents inserted into the db.
        """
        for doc_id, doc_rev, doc in docs_info:
            self.force_doc_with_conflict(doc_id, doc_rev, doc)
        return len(docs_info)

    def sync(self, other, callback=None):
        other_st = other.get_sync_target()
        (other_machine_id, other_rev,
         others_my_rev) = other_st.get_sync_info(self._machine_id)
        docs_to_send = []
        my_db_rev, changed_doc_ids = self.whats_changed(others_my_rev)
        docs_to_send = self.get_docs(changed_doc_ids)
        other_last_known_rev = self.get_sync_generation(other_machine_id)
        (new_records, conflicted_records,
         new_db_rev) = other_st.sync_exchange(docs_to_send, self._machine_id,
                            my_db_rev, other_last_known_rev)
        all_records = new_records + conflicted_records
        conflict_ids, _, num_inserted = self.put_docs(all_records)
        conflict_docs = [r for r in all_records if r[0] in conflict_ids]
        num_inserted += self._insert_conflicts(conflict_docs)
        self.set_sync_generation(other_machine_id, new_db_rev)
        cur_db_rev = self._get_db_rev()
        if cur_db_rev == my_db_rev + num_inserted:
            other_st.record_sync_info(self._machine_id, cur_db_rev)
        return my_db_rev

    def _ensure_maximal_rev(self, cur_rev, extra_revs):
        vcr = VectorClockRev(cur_rev)
        for rev in extra_revs:
            vcr.maximize(VectorClockRev(rev))
        vcr.increment(self._machine_id)
        return vcr.as_str()
