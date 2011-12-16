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

import re
import uuid

import u1db
from u1db import (
    errors,
)
import u1db.sync
from u1db.vectorclock import VectorClockRev


check_doc_id_re = re.compile("^" + u1db.DOC_ID_CONSTRAINTS + "$")


class CommonSyncTarget(u1db.sync.LocalSyncTarget):
    pass


class CommonBackend(u1db.Database):

    def _allocate_doc_id(self):
        """Generate a unique identifier for this document."""
        return 'D-' + uuid.uuid4().hex  # 'D-' stands for document

    def _allocate_doc_rev(self, old_doc_rev):
        vcr = VectorClockRev(old_doc_rev)
        vcr.increment(self._replica_uid)
        return vcr.as_str()

    def _check_doc_id(self, doc_id):
        if not check_doc_id_re.match(doc_id):
            raise errors.InvalidDocId()

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

    def get_docs(self, doc_ids, check_for_conflicts=True):
        result = []
        for doc_id in doc_ids:
            doc = self._get_doc(doc_id)
            if check_for_conflicts:
                doc.has_conflicts = self._has_conflicts(doc_id)
            result.append(doc)
        return result

    def put_doc_if_newer(self, doc, save_conflict=False, replica_uid=None,
                         replica_gen=None):
        cur_doc = self._get_doc(doc.doc_id)
        doc_vcr = VectorClockRev(doc.rev)
        if cur_doc is None:
            cur_vcr = VectorClockRev(None)
        else:
            cur_vcr = VectorClockRev(cur_doc.rev)
        if doc_vcr.is_newer(cur_vcr):
            self._put_and_update_indexes(cur_doc, doc)
            self._prune_conflicts(doc, doc_vcr)
            state = 'inserted'
        elif doc.rev == cur_doc.rev:
            # magical convergence
            state = 'converged'
        elif cur_vcr.is_newer(doc_vcr):
            # Don't add this to seen_ids, because we have something newer,
            # so we should send it back, and we should not generate a
            # conflict
            state = 'superseded'
        else:
            state = 'conflicted'
            if save_conflict:
                self._force_doc_sync_conflict(doc)
        if replica_uid is not None and replica_gen is not None:
            self._set_sync_generation(replica_uid, replica_gen)
        return state

    def _ensure_maximal_rev(self, cur_rev, extra_revs):
        vcr = VectorClockRev(cur_rev)
        for rev in extra_revs:
            vcr.maximize(VectorClockRev(rev))
        vcr.increment(self._replica_uid)
        return vcr.as_str()
