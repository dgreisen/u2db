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

"""U1DB"""

__version_info__ = (0, 0, 1, 'dev', 0)
__version__ = '.'.join(map(str, __version_info__))

class Database(object):
    """A JSON Document data store.

    This data store can be synchronized with other u1db.Database instances.
    """

    def whats_changed(self, old_db_rev):
        """Return a list of entries that have changed since old_db_rev.
        This allows APPS to only store a db_rev before going 'offline', and
        then when coming back online they can use this data to update whatever
        extra data they are storing.

        :param old_db_rev: The global revision state of the database in the old
            state.
        :return: (cur_db_rev, set([doc_id]))
            The current global revision state of the database, and the set of
            document ids that were changed in between old_db_rev and cur_db_rev
        """
        raise NotImplementedError(self.whats_changed)

    def get_doc(self, doc_id):
        """Get the JSON string for the given document.

        :param doc_id: The unique document identifier
        :return: (doc_rev, doc, has_conflicts)
            doc_rev- The current version of the document
            doc- A JSON string if the document exists (possibly an empty
                string), None/nil if the document does not exist.
            has_conflicts- A boolean indicating if there are conflict records
                for this document
        """
        raise NotImplementedError(self.get_doc)

    def get_docs(self, doc_ids):
        """Get the JSON content for many documents.

        :param doc_ids: A list of document identifiers.
        :return: [(doc_rev, doc)] for each document listed, note that this
            ignores conflicts.
        """
        raise NotImplementedError(self.get_docs)

    def create_doc(self, doc, doc_id=None):
        """Create a new document.

        You can optionally specify the document identifier, but the document
        must not already exist. See 'put_doc' if you want to override an
        existing document.
        :param doc: The JSON document string
        :param doc_id: An optional identifier specifying the document id.
        :return: (doc_id, doc_rev) The identifier used for the document, and
            the current revision identifier for it.
        """
        raise NotImplementedError(self.create_doc)

    def put_doc(self, doc_id, old_doc_rev, doc):
        """Update a document.
        If the document currently has conflicts, put will fail.

        :param doc_id: Unique handle for a document, if it is None, a new
            identifier will be allocated for you.
        :param old_doc_rev: The document revision that we know to be
            superseding. If 'old_doc_rev' doesn't actually match the current
            doc_rev, the put fails, indicating there is a newer version stored.
        :param doc: The actual JSON document string.
        :return: new_doc_rev - The new revision identifier for the document
        """
        raise NotImplementedError(self.put_doc)

    def put_docs(self, docs_info):
        """Insert/update many documents into the database.

        This api is used during synchronization operations. It is possible to
        also use for client code, but put_doc() is more obvious.

        :param: A list of [(doc_id, doc_rev, doc_content)]. If we don't have
            doc_id already, or if doc_rev supersedes the existing document
            revision, then the content will be inserted, and num_inserted will
            be incremented.
            If doc_rev is less than or equal to the existing revision, then the
            put is ignored.
            If doc_rev is not strictly superseded or supersedes, then the
            document id is added to the set of conflicted documents.
        :return: (would_conflict_ids, superseded_ids, num_inserted), the
            document_ids that that could not be inserted (and were not
            superseded), the document ids that we already had something newer,
            and the count of entries that were successfully added.
        """
        raise NotImplementedError(self.put_docs)

    def force_doc_with_conflict(self, doc_id, doc_rev, doc):
        """Update documents even though they should conflict.

        This is used for synchronization, and should generally not be used by
        clients.

        The content will be selected as the 'current' content for doc_id, even
        though doc_rev may not supersede the currently stored revision.  The
        currently stored document will be added to the list of conflict
        alternatives for the given doc_id.

        The reason this forces the new content to be 'current' is so that we
        get convergence after synchronizing, even if people don't resolve
        conflicts. Users can then notice that their content is out of date,
        update it, and synchronize again. (The alternative is that users could
        synchronize and think the data has propagated, but their local copy
        looks fine, and the remote copy is never updated again.)

        :param doc_id: The indentifier for this document
        :param doc_rev: The document revision for this document
        :param doc: The JSON string for the document.
        :return: None
        """
        raise NotImplementedError(self.force_doc_with_conflict)

    def delete_doc(self, doc_id, old_doc_rev):
        """Mark a document as deleted.
        (might be equivalent to PUT(nil)). Will abort if the document is now
        'newer' than old_doc_rev.
        """
        raise NotImplementedError(self.delete_doc)

    def create_index(self, index_name, index_expression):
        """Create an named index, which can then be queried for future lookups.
        Creating an index which already exists is not an error, and is cheap.
        Creating an index which does not match the index_expressions of the
        existing index is an error.
        Creating an index will block until the expressions have been evaluated
        and the index generated.

        :name: A unique name which can be used as a key prefix
        :index_expressions: A list of index expressions defining the index
            information. Examples:
                ["field"] to index alphabetically sorted on field.
                ["number(field, bits)", "lower(field)", "field.subfield"]
        """
        raise NotImplementedError(self.create_index)

    def delete_index(self, index_name):
        """Remove a named index.

        :param index_name: The name of the index we are removing
        :return: None
        """
        raise NotImplementedError(self.delete_index)

    def list_indexes(self):
        """List the definitions of all known indexes.

        :return: A list of [('index-name', ['field', 'field2'])] definitions.
        """
        raise NotImplementedError(self.list_indexes)

    def get_from_index(self, index_name, key_values):
        """Return documents that match the keys supplied.

        You must supply exactly the same number of values as the index has been
        defined. It is possible to do a prefix match by using '*' to indicate a
        wildcard match. You can only supply '*' to trailing entries, (eg
        [('val', '*', '*')] is allowed, but [('*', 'val', 'val')] is not.)
        It is also possible to append a '*' to the last supplied value (eg
        [('val*', '*', '*')] or [('val', 'val*', '*')], but not
        [('val*', 'val', '*')])

        :return: List of [(doc_id, doc_rev, doc)]
        :param index_name: The index to query
        :param key_values: A list of tuple of values to match. eg, if you have
            an index with 3 field,s then you would have:
            [(x-val1, x-val2, x-val3), (y-val1, y-val2, y-val3), ...])
        """
        raise NotImplementedError(self.get_from_index)

    def get_doc_conflicts(self, doc_id):
        """Get the list of conflict texts for the given document.

        The order of the conflicts is such that the first entry is the value
        that would be returned by "get_doc".

        :return: [(doc_rev, doc)] a list of tuples of the revision for the
            content, and the JSON string of the content.
        """
        raise NotImplementedError(self.get_doc_conflicts)

    def resolve_doc(self, doc_id, doc, conflicted_doc_revs):
        """Mark a document as no longer conflicted.
        We take the list of revisions that the client knows about that it is
        superseding. This may be a different list from the actual current
        conflicts, in which case only those are removed as conflicted.  This
        may fail if the conflict list is significantly different from the
        supplied information. (sync could have happened in the background from
        the time you GET_DOC_CONFLICTS until the point where you RESOLVE)

        :return: (new_rev, still_conflicted)
        """
        raise NotImplementedError(self.resolve_doc)

    def get_sync_target(self):
        """Return a SyncTarget object, for another u1db to synchronize with.

        :return: An instance of SyncTarget.
        """
        raise NotImplementedError(self.get_sync_target)

    def get_sync_generation(self, other_db_id):
        """Return the last known database generation of 'other'.

        When you do a synchronization with another database, the Database keeps
        track of what generation the other database was at. This way we only
        have to request data that is newer.

        :param other_db_id: The identifier for the other database.
        :return: The generation we encountered during synchronization. If we've
            never synchronized with the machine, this is 0.
        """
        raise NotImplementedError(self.get_sync_generation)

    def set_sync_generation(self, other_db_id, other_generation):
        """Set the last-known generation for the other database.

        We have just performed some synchronization, and we want to track what
        generation the other database was at. See also get_sync_generation.
        :param other_db_id: The U1DB identifier for the other database.
        :param other_generation: The generation number for the other db.
        :return: None
        """
        raise NotImplementedError(self.get_sync_generation)


class SyncTarget(object):
    """Functionality for using a Database as a synchronization target."""

    def get_sync_info(self, other_machine_id):
        """Return information about known state.

        Return the machine_id and the current database generation of this
        database, and the last-seen database generation for other_machine_id

        :param other_machine_id: Another machine which we might have
            synchronized with in the past.
        :return: (this_machine_id, this_machine_db_rev,
                  other_machine_last_known_generation)
        """
        raise NotImplementedError(self.get_sync_info)

    def record_sync_info(self, other_machine_id, other_machine_gen):
        """Record tip information for another machine.

        After sync_exchange has been processed, the caller will have received
        new content from this machine. This call allows the machine instigating
        the sync to inform us what their global database revision became after
        applying the documents we returned.

        This is used to allow future sync operations to not need to repeat data
        that we just talked about. It also means that if this is called at the
        wrong time, there can be database records that will never be
        synchronized.

        :param other_machine_id: The identifier for the other machine.
        :param other_machine_rev: The database revision for other_machine
        :return: None
        """
        raise NotImplementedError(self.record_sync_info)

    def sync_exchange(self, docs_info, from_machine_id, from_machine_rev,
                      last_known_rev):
        """Incorporate the documents sent from the other machine.

        This is not meant to be called by client code directly, but is used as
        part of sync().

        This adds docs to the local store, and determines documents that need
        to be returned to the other machine.

        :param docs_info: A list of [(doc_id, doc_rev, doc)] tuples indicating
            documents which should be updated on this machine.
        :param from_machine_id: The other machines' identifier
        :param from_machine_rev: The db rev for the other machine, indicating
            the tip of data being sent by docs_info.
        :param last_known_rev: The last db_rev that other_machine knows about
            this
        :return: (new_records, conflicted_records, new_db_rev)
            new_records - A list of [(doc_id, doc_rev, doc)] that have changed
                          since other_my_rev
            conflicted_records - A list of [(doc_id, doc_rev, doc)] for entries
                which were sent in docs_info, but which cannot be applied
                because it would conflict.
            new_db_rev - After applying docs_info, this is the current db_rev
                for this client
        """
        raise NotImplementedError(self.sync_exchange)

