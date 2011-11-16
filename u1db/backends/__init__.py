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
import u1db.sync
from u1db.vectorclock import VectorClockRev


class CommonSyncTarget(u1db.sync.LocalSyncTarget):
    pass


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

    def create_doc(self, content, doc_id=None):
        if doc_id is None:
            doc_id = self._allocate_doc_id()
        doc = u1db.Document(doc_id, None, content)
        self.put_doc(doc)
        return doc

    def _get_transaction_log(self):
        """This is only for the test suite, it is not part of the api."""
        raise NotImplementedError(self._get_transaction_log)

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, content):
        raise NotImplementedError(self._put_and_update_indexes)

    def _compare_and_insert_doc(self, doc_id, doc_rev, content):
        """Check if a document is newer than current and insert it.

        :return: (old_doc, state)
        """
        cur_doc = self._get_doc(doc_id)
        doc_vcr = VectorClockRev(doc_rev)
        if cur_doc is None:
            cur_vcr = VectorClockRev(None)
            cur_content = None
        else:
            cur_vcr = VectorClockRev(cur_doc.rev)
            cur_content = cur_doc.content
        if doc_vcr.is_newer(cur_vcr):
            self._put_and_update_indexes(doc_id, cur_content, doc_rev, content)
            return cur_doc, 'inserted'
        elif doc_rev == cur_doc.rev:
            # magical convergence
            return cur_doc.content, 'converged'
        elif cur_vcr.is_newer(doc_vcr):
            # Don't add this to seen_ids, because we have something newer,
            # so we should send it back, and we should not generate a
            # conflict
            return cur_doc.content, 'superseded'
        else:
            return cur_doc.content, 'conflicted'

    def get_docs(self, doc_ids, check_for_conflicts=True):
        result = []
        for doc_id in doc_ids:
            doc = self._get_doc(doc_id)
            if check_for_conflicts:
                is_conflicted = self._has_conflicts(doc_id)
            else:
                is_conflicted = None
            result.append((doc_id, doc.rev, doc.content, is_conflicted))
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
