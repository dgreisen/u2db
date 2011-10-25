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
    """Collect the state around synchronizing 2 U1DB instances.

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

    def _insert_conflicts(self, docs_info):
        """Record all of docs_info as conflicted documents.

        Because of the 'TAKE_OTHER' semantics, any document which is marked as
        conflicted takes docs_info as the official value.
        This will update index definitions, etc.

        :return: The number of documents inserted into the db.
        """
        for doc_id, doc_rev, doc in docs_info:
            self.source.force_doc_with_conflict(doc_id, doc_rev, doc)
        return len(docs_info)

    def sync(self, callback=None):
        sync_target = self.sync_target
        (other_machine_id, other_rev,
         others_my_rev) = sync_target.get_sync_info(self.source._machine_id)
        docs_to_send = []
        my_db_rev, changed_doc_ids = self.source.whats_changed(others_my_rev)
        docs_to_send = self.source.get_docs(changed_doc_ids)
        other_last_known_rev = self.source.get_sync_generation(other_machine_id)
        (new_records, conflicted_records,
         new_db_rev) = sync_target.sync_exchange(docs_to_send,
            self.source._machine_id, my_db_rev, other_last_known_rev)
        all_records = new_records + conflicted_records
        conflict_ids, _, num_inserted = self.source.put_docs_if_newer(
            all_records)
        conflict_docs = [r for r in all_records if r[0] in conflict_ids]
        num_inserted += self._insert_conflicts(conflict_docs)
        self.source.set_sync_generation(other_machine_id, new_db_rev)
        cur_db_rev = self.source._get_db_rev()
        if cur_db_rev == my_db_rev + num_inserted:
            sync_target.record_sync_info(self.source._machine_id, cur_db_rev)
        return my_db_rev

