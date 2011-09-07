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

"""The Client class for U1DB."""

import re


class Client(object):

    def sync(self, callback):
        """Synchronize my database with the remote database.
        Does not (yet) support a peer, so sync is defined only to U1. This
        pushes local changes to the remote, and pulls remote changes locally.
        There is not a separate push vs pull step.

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

    def put_doc(self, doc_id, old_doc_rev, doc):
        """Add/update a document.
        If the document currently has conflicts, put will fail.

        :param doc_id: Unique handle for a document
        :param old_doc_rev: The document revision that we know to be
            superseding. If 'old_doc_rev' doesn't actually match the current
            doc_rev, the put fails, indicating there is a newer version stored.
        :param doc: The actual JSON document string.
        :return: (doc_id, new_doc_rev) Returns the new revision string for the
            document.
        """
        raise NotImplementedError(self.put_doc)

    def delete_doc(self, doc_id, old_doc_rev):
        """Mark a document as deleted.
        (might be equivalent to PUT(nil)). Will abort if the document is now
        'newer' than old_doc_rev.
        """
        raise NotImplementedError(self.delet_doc)


class InMemoryClient(Client):
    """A client that only stores the data internally."""

    def __init__(self):
        self._docs = {}
        self._doc_counter = 0
        self._machine_id = 'test'

    def _allocate_doc_id(self):
        self._doc_counter += 1
        return 'doc-%d' % (self._doc_counter,)

    def _allocate_doc_rev(self, old_doc_rev):
        if old_doc_rev is None:
            return self._machine_id + ':1'
        result = old_doc_rev.split('|')
        for idx, machine_counter in enumerate(result):
            machine_id, counter = machine_counter.split(':')
            if machine_id == self._machine_id:
                result[idx] = '%s:%d' % (machine_id, int(counter) + 1)
                break
        else:
            result.append('%s:%d' % (self._machine_id, 1))
        return '|'.join(result)

    def put_doc(self, doc_id, old_doc_rev, doc):
        if doc_id is None:
            doc_id = self._allocate_doc_id()
        new_rev = self._allocate_doc_rev(old_doc_rev)
        self._docs[doc_id] = (new_rev, doc)
        return doc_id, new_rev

    def get_doc(self, doc_id):
        doc_rev, doc = self._docs[doc_id]
        return doc_rev, doc, False
