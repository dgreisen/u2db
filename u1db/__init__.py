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

"""U1DB"""


class Database(object):

    def sync(self, other, callback):
        """Synchronize my database with another database.
        This pushes local changes to the remote, and pulls remote changes
        locally.  There is not a separate push vs pull step.

        :param other: Another database to sync with
        :param callback: gives optional progress callbacks
        :return: db_revid for the new global last-modified-db-info
        """
        raise NotImplementedError(self.sync)

    def whats_changed(self, old_db_rev):
        """Return a list of entries that have changed since old_db_rev.
        This allows APPS to only store a db_rev before going 'offline', and
        then when coming back online they can use this data to update whatever
        extra data they are storing.

        :param old_db_rev: The global revision state of the database in the old
            state.
        :return: A list of [doc_id] that have changed since db_rev.
        """
        raise NotImplementedError(self.whats_changed)

    def get_doc(self, doc_id):
        """Get the JSON string for the given document.

        :param doc_id: The unique document identifier
        :return: (doc_id, doc_rev, has_conflicts, doc)

            :doc_rev- The current version of the document
            :has_conflicts- A boolean indicating if there are conflict records
                for this document
            :doc- A JSON string if the document exists (possibly an empty
                string), None/nil if the document does not exist.
        """
        raise NotImplementedError(self.get_doc)

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
        """Add/update a document.
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

    def get_from_index(self, index_name, key_values):
        """Return documents that match the exact keys supplied.

        :return: List of [(doc_id, doc_rev, doc)]
        :param index_name: The index to query
        :param key_values: A list of tuple of values to match. eg, if you have
        """
        raise NotImplementedError(self.get_from_index)

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


class InvalidDocRev(Exception):
    """The document revisions supplied does not match the current version."""


class InvalidDocId(Exception):
    """A document was tried with an invalid document identifier."""


class ConflictedDoc(Exception):
    """The document is conflicted, you must call resolve before put()"""

