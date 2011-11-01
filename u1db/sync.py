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

    def _insert_other_doc(self, doc_id, doc_rev, doc):
        """Try to insert synced over document.

        Because of the 'TAKE_OTHER' semantics, any document which is
        marked as conflicted takes the other value as the official
        value.  This will update index definitions, etc. Increases
        self.num_inserted depending whether the document
        was effectively inserted.

        :return: None
        """
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
                        return_doc_cb=self._insert_other_doc)
        self.source.set_sync_generation(other_replica_uid, new_gen)
        cur_gen = self.source._get_generation()
        if cur_gen == my_gen + self.num_inserted:
            sync_target.record_sync_info(self.source._replica_uid, cur_gen)
        return my_gen
