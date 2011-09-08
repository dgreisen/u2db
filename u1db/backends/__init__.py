# Copyright (C) 2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

""""""

import u1db
from u1db.vectorclock import VectorClockRev


class CommonBackend(u1db.Database):

    def _allocate_doc_id(self):
        """Generate a unique identifier for this document."""
        raise NotImplementedError(self._allocate_doc_id)

    def _allocate_doc_rev(self, old_doc_rev):
        vcr = VectorClockRev(old_doc_rev)
        return vcr.increment(self._machine_id)

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
        cur_rev, cur_doc, _ = self.get_doc(doc_id)
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

    def _insert_many_docs(self, docs_info):
        """Add a bunch of documents to the local store.

        This will only add entries if they supersede the local entries,
        otherwise the doc ids will be added to conflict_ids.
        :param docs_info: List of [(doc_id, doc_rev, doc)]
        :return: (seen_ids, conflict_ids, num_inserted) sets of entries that
            were seen, and what was considered conflicted and not added, and
            the number of documents that were explictly added.
        """
        conflict_ids = set()
        seen_ids = set()
        num_inserted = 0
        for doc_id, doc_rev, doc in docs_info:
            old_doc, state = self._compare_and_insert_doc(doc_id, doc_rev, doc)
            seen_ids.add(doc_id)
            if state == 'inserted':
                num_inserted += 1
            elif state == 'converged':
                # magical convergence
                continue
            elif state == 'superseded':
                # Don't add this to seen_ids, because we have something newer,
                # so we should send it back, and we should not generate a
                # conflict
                seen_ids.remove(doc_id)
                continue
            else:
                assert state == 'conflicted'
                conflict_ids.add(doc_id)
        return seen_ids, conflict_ids, num_inserted

    def _sync_exchange(self, docs_info, from_machine_id, from_machine_rev,
                       last_known_rev):
        seen_ids, conflict_ids, _ = self._insert_many_docs(docs_info)
        new_docs = []
        my_db_rev, changed_doc_ids = self.whats_changed(last_known_rev)
        for doc_id in changed_doc_ids:
            if doc_id in seen_ids:
                continue
            doc_rev, doc, _ = self.get_doc(doc_id)
            new_docs.append((doc_id, doc_rev, doc))
        conflicts = []
        for doc_id in conflict_ids:
            doc_rev, doc, _ = self.get_doc(doc_id)
            conflicts.append((doc_id, doc_rev, doc))
        self._record_sync_info(from_machine_id, from_machine_rev)
        self._last_exchange_log = {
            'receive': {'docs': [(di, dr) for di, dr, _ in docs_info],
                        'from_id': from_machine_id,
                        'from_rev': from_machine_rev,
                        'last_known_rev': last_known_rev},
            'return': {'new_docs': [(di, dr) for di, dr, _ in new_docs],
                       'conf_docs': [(di, dr) for di, dr, _ in conflicts],
                       'last_rev': my_db_rev}
        }
        return new_docs, conflicts, my_db_rev

