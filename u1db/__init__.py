# Copyright 2011 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

"""U1DB"""

__version_info__ = (0, 0, 1, 'dev', 0)
__version__ = '.'.join(map(str, __version_info__))


def open(path, create):
    """Open a database at the given location.

    Will raise u1db.errors.DatabaseDoesNotExist if create=False and the
    database does not already exist.

    :param path: The filesystem path for the database to open.
    :param create: True/False, should the database be created if it doesn't
        already exist?
    :return: An instance of Database.
    """
    from u1db.backends import sqlite_backend
    return sqlite_backend.SQLiteDatabase.open_database(path, create=create)


# constraints on database names (relevant for remote access, as regex)
DBNAME_CONSTRAINTS = r"[a-zA-Z0-9][a-zA-Z0-9.-]*"

# constraints on doc ids (as regex)
DOC_ID_CONSTRAINTS = r"[^/\\]+"


class Database(object):
    """A JSON Document data store.

    This data store can be synchronized with other u1db.Database instances.
    """

    def whats_changed(self, old_generation):
        """Return a list of documents that have changed since old_generation.
        This allows APPS to only store a db generation before going
        'offline', and then when coming back online they can use this
        data to update whatever extra data they are storing.

        :param old_generation: The generation of the database in the old
            state.
        :return: (cur_generation, [(doc_id, generation),...])
            The current generation of the database, and a list of of
            changed documents since old_generation, represented by tuples
            with for each document its doc_id and the generation corresponding
            to the last intervening change and sorted by generation
        """
        raise NotImplementedError(self.whats_changed)

    def get_doc(self, doc_id):
        """Get the JSON string for the given document.

        :param doc_id: The unique document identifier
        :return: a Document object.
        """
        raise NotImplementedError(self.get_doc)

    def get_docs(self, doc_ids, check_for_conflicts=True):
        """Get the JSON content for many documents.

        :param doc_ids: A list of document identifiers.
        :param check_for_conflicts: If set to False, then the conflict check
            will be skipped, and 'None' will be returned instead of True/False.
        :return: [Document] for each document id and matching doc_ids order.
        """
        raise NotImplementedError(self.get_docs)

    def create_doc(self, content, doc_id=None):
        """Create a new document.

        You can optionally specify the document identifier, but the document
        must not already exist. See 'put_doc' if you want to override an
        existing document.
        :param content: The JSON document string
        :param doc_id: An optional identifier specifying the document id.
        :return: Document
        """
        raise NotImplementedError(self.create_doc)

    def put_doc(self, doc):
        """Update a document.
        If the document currently has conflicts, put will fail.

        :param doc: A Document with new content.
        :return: new_doc_rev - The new revision identifier for the document.
            The Document object will also be updated.
        """
        raise NotImplementedError(self.put_doc)

    def put_doc_if_newer(self, doc, save_conflict, replica_uid=None,
                         replica_gen=None):
        """Insert/update document into the database with a given revision.

        This api is used during synchronization operations.

        If a document would conflict and save_conflict is set to True, the
        content will be selected as the 'current' content for doc.doc_id,
        even though doc.rev doesn't supersede the currently stored revision.
        The currently stored document will be added to the list of conflict
        alternatives for the given doc_id.

        This forces the new content to be 'current' so that we get convergence
        after synchronizing, even if people don't resolve conflicts. Users can
        then notice that their content is out of date, update it, and
        synchronize again. (The alternative is that users could synchronize and
        think the data has propagated, but their local copy looks fine, and the
        remote copy is never updated again.)

        :param doc: A Document object
        :param save_conflict: If this document is a conflict, do you want to
            save it as a conflict, or just ignore it.
        :param replica_uid: A unique replica identifier.
        :param replica_gen: The generation of the replica corresponding to the
            this document. The replica arguments are optional, but are used
            during synchronization.
        :return: state -  If we don't have doc_id already, or if doc_rev
            supersedes the existing document revision, then the content will
            be inserted, and state is 'inserted'.
            If doc_rev is less than or equal to the existing revision,
            then the put is ignored and state is respecitvely 'superseded'
            or 'converged'.
            If doc_rev is not strictly superseded or supersedes, then
            state is 'conflicted'. The document will not be inserted if
            save_conflict is False.
        """
        raise NotImplementedError(self.put_doc_if_newer)

    def delete_doc(self, doc):
        """Mark a document as deleted.
        Will abort if the current revision doesn't match doc.rev.
        This will also set doc.content to None.
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

        :return: List of [Document]
        :param index_name: The index to query
        :param key_values: A list of tuple of values to match. eg, if you have
            an index with 3 field,s then you would have:
            [(x-val1, x-val2, x-val3), (y-val1, y-val2, y-val3), ...])
        """
        raise NotImplementedError(self.get_from_index)

    # XXX: get_doc_conflicts still uses tuples, we need to change this to using
    #      Document objects
    def get_doc_conflicts(self, doc_id):
        """Get the list of conflict texts for the given document.

        The order of the conflicts is such that the first entry is the value
        that would be returned by "get_doc".

        :return: [doc] A list of the Document entries that are conflicted.
        """
        raise NotImplementedError(self.get_doc_conflicts)

    def resolve_doc(self, doc, conflicted_doc_revs):
        """Mark a document as no longer conflicted.

        We take the list of revisions that the client knows about that it is
        superseding. This may be a different list from the actual current
        conflicts, in which case only those are removed as conflicted.  This
        may fail if the conflict list is significantly different from the
        supplied information. (sync could have happened in the background from
        the time you GET_DOC_CONFLICTS until the point where you RESOLVE)

        :param doc: A Document with the new content to be inserted.
        :param conflicted_doc_revs: A list of revisions that the new content
            supersedes.
        :return: None, doc will be updated with the new revision and
            has_conflict flags.
        """
        raise NotImplementedError(self.resolve_doc)

    def get_sync_target(self):
        """Return a SyncTarget object, for another u1db to synchronize with.

        :return: An instance of SyncTarget.
        """
        raise NotImplementedError(self.get_sync_target)

    def get_sync_generation(self, other_replica_uid):
        """Return the last known database generation of the other db replica.

        When you do a synchronization with another replica, the Database keeps
        track of what generation the other database replica  was at.
        This way we only have to request data that is newer.

        :param other_replica_uid: The identifier for the other replica.
        :return: The generation we encountered during synchronization. If we've
            never synchronized with the replica, this is 0.
        """
        raise NotImplementedError(self.get_sync_generation)

    def set_sync_generation(self, other_replica_uid, other_generation):
        """Set the last-known generation for the other database replica.

        We have just performed some synchronization, and we want to track what
        generation the other replica was at. See also get_sync_generation.
        :param other_replica_uid: The U1DB identifier for the other replica.
        :param other_generation: The generation number for the other replica.
        :return: None
        """
        raise NotImplementedError(self.get_sync_generation)

    def close(self):
        """Release any resources associated with this database."""
        raise NotImplementedError(self.close)

    def sync(self, url):
        """Synchronize documents with remote replica exposed at url."""
        from u1db.sync import Synchronizer
        from u1db.remote.http_target import HTTPSyncTarget
        return Synchronizer(self, HTTPSyncTarget(url)).sync()


class Document(object):
    """Container for handling a single document.

    :ivar doc_id: Unique identifier for this document.
    :ivar rev:
    :ivar content: The JSON string for this document.
    :ivar has_conflicts: Boolean indicating if this document has conflicts
    """

    def __init__(self, doc_id, rev, content, has_conflicts=False):
        self.doc_id = doc_id
        self.rev = rev
        self.content = content
        self.has_conflicts = has_conflicts

    def __repr__(self):
        if self.has_conflicts:
            extra = ', conflicted'
        else:
            extra = ''
        return '%s(%s, %s%s, %r)' % (self.__class__.__name__, self.doc_id,
                                     self.rev, extra, self.content)

    def __hash__(self):
        raise NotImplementedError(self.__hash__)

    def __eq__(self, other):
        if not isinstance(other, Document):
            return False
        return self.__dict__ == other.__dict__

    def __lt__(self, other):
        """This is meant for testing, not part of the official api.

        It is implemented so that sorted([Document, Document]) can be used.
        It doesn't imply that users would want their documents to be sorted in
        this order.
        """
        # Since this is just for testing, we don't worry about comparing
        # against things that aren't a Document.
        return ((self.doc_id, self.rev, self.content)
            < (other.doc_id, other.rev, other.content))


class SyncTarget(object):
    """Functionality for using a Database as a synchronization target."""

    def get_sync_info(self, source_replica_uid):
        """Return information about known state.

        Return the replica_uid and the current database generation of this
        database, and the last-seen database generation for source_replica_uid

        :param source_replica_uid: Another replica which we might have
            synchronized with in the past.
        :return: (target_replica_uid, target_replica_generation,
                  source_replica_last_known_generation)
        """
        raise NotImplementedError(self.get_sync_info)

    def record_sync_info(self, source_replica_uid, source_replica_generation):
        """Record tip information for another replica.

        After sync_exchange has been processed, the caller will have
        received new content from this replica. This call allows the
        source replica instigating the sync to inform us what their
        generation became after applying the documents we returned.

        This is used to allow future sync operations to not need to repeat data
        that we just talked about. It also means that if this is called at the
        wrong time, there can be database records that will never be
        synchronized.

        :param source_replica_uid: The identifier for the source replica.
        :param source_replica_generation:
             The database generation for the source replica.
        :return: None
        """
        raise NotImplementedError(self.record_sync_info)

    def sync_exchange(self, docs_by_generation, source_replica_uid,
                      last_known_generation, return_doc_cb):
        """Incorporate the documents sent from the source replica.

        This is not meant to be called by client code directly, but is used as
        part of sync().

        This adds docs to the local store, and determines documents that need
        to be returned to the source replica.

        Documents must be supplied in docs_by_generation paired with
        the generation of their latest change in order from the oldest
        change to the newest, that means from the oldest generation to
        the newest.

        Documents are also returned paired with the generation of
        their latest change in order from the oldest change to the
        newest.

        :param docs_by_generation: A list of [(Document, generation)]
              pairs indicating documents which should be updated on
              this replica paired with the generation of their
              latest change.
        :param source_replica_uid: The source replica's identifier
        :param last_known_generation: The last generation that the source
            replica knows about this
        :param: return_doc_cb(doc, gen): is a callback
                used to return documents to the source replica, it will
                be invoked in turn with Documents that have changed since
                last_known_generation together with the generation of
                their last change.
        :return: new_generation - After applying docs_by_generation, this is
            the current generation for this replica
        """
        raise NotImplementedError(self.sync_exchange)

    def _set_trace_hook(self, cb):
        """Set a callback that will be invoked to trace database actions.

        The callback will be passed a string indicating the current state, and
        the sync target object.  Implementations do not have to implement this
        api, it is used by the test suite.

        :param cb: A callable that takes cb(state)
        """
        raise NotImplementedError(self._set_trace_hook)
