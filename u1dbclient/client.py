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
import simplejson


class Client(object):

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

    def put_doc(self, doc_id, old_doc_rev, doc):
        """Add/update a document.
        If the document currently has conflicts, put will fail.

        :param doc_id: Unique handle for a document, if it is None, a new
            identifier will be allocated for you.
        :param old_doc_rev: The document revision that we know to be
            superseding. If 'old_doc_rev' doesn't actually match the current
            doc_rev, the put fails, indicating there is a newer version stored.
        :param doc: The actual JSON document string.
        :return: (doc_id, new_doc_rev, new_db_rev) Returns the new revision
            string for the document.
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


class InvalidDocRev(Exception):
    """The document revisions supplied does not match the current version."""


class ConflictedDoc(Exception):
    """The document is conflicted, you must call resolve before put()"""


class InMemoryClient(Client):
    """A client that only stores the data internally."""

    def __init__(self, machine_id):
        self._transaction_log = []
        self._docs = {}
        # Map from doc_id => [(doc_rev, doc)] conflicts beyond 'winner'
        self._conflicts = {}
        self._other_revs = {}
        self._indexes = {}
        self._doc_counter = 0
        self._machine_id = machine_id

    def get_sync_info(self, other_machine_id):
        other_rev = self._other_revs.get(other_machine_id, 0)
        return self._machine_id, len(self._transaction_log), other_rev

    def _get_other_machine_rev(self, other_machine_id):
        return self._other_revs.get(other_machine_id, 0)

    def put_state_info(self, machine_id, db_rev):
        self._other_revs[machine_id] = db_rev

    def _allocate_doc_id(self):
        self._doc_counter += 1
        return 'doc-%d' % (self._doc_counter,)

    def _allocate_doc_rev(self, old_doc_rev):
        vcr = VectorClockRev(old_doc_rev)
        return vcr.increment(self._machine_id)

    def put_doc(self, doc_id, old_doc_rev, doc):
        if doc_id is None:
            doc_id = self._allocate_doc_id()
        old_doc = None
        if doc_id in self._docs:
            if doc_id in self._conflicts:
                raise ConflictedDoc()
            old_rev, old_doc = self._docs[doc_id]
            if old_rev != old_doc_rev:
                raise InvalidDocRev()
        new_rev = self._allocate_doc_rev(old_doc_rev)
        for index in self._indexes.itervalues():
            if old_doc is not None:
                index.remove_json(doc_id, old_doc)
            index.add_json(doc_id, doc)
        self._docs[doc_id] = (new_rev, doc)
        self._transaction_log.append(doc_id)
        return doc_id, new_rev, len(self._transaction_log)

    def get_doc(self, doc_id):
        try:
            doc_rev, doc = self._docs[doc_id]
        except KeyError:
            return None, None, False
        return doc_rev, doc, (doc_id in self._conflicts)

    def _get_current_rev(self, doc_id):
        return self._docs.get(doc_id, (None, None))[0]

    def delete_doc(self, doc_id, doc_rev):
        cur_doc_rev, old_doc = self._docs[doc_id]
        if doc_rev != cur_doc_rev:
            raise InvalidDocRev()
        for index in self._indexes.itervalues():
            index.remove_json(doc_id, old_doc)
        del self._docs[doc_id]
        self._transaction_log.append(doc_id)

    def create_index(self, index_name, index_expression):
        index = InMemoryIndex(index_name, index_expression)
        for doc_id, (doc_rev, doc) in self._docs.iteritems():
            index.add_json(doc_id, doc)
        self._indexes[index_name] = index

    def delete_index(self, index_name):
        del self._indexes[index_name]

    def get_from_index(self, index_name, key_values):
        index = self._indexes[index_name]
        doc_ids = index.lookup(key_values)
        result = []
        for doc_id in doc_ids:
            doc_rev, doc = self._docs[doc_id]
            result.append((doc_id, doc_rev, doc))
        return result

    def whats_changed(self, old_db_rev):
        return set(self._transaction_log[old_db_rev:])

    def _insert_many_docs(self, docs_info):
        conflict_ids = []
        seen_ids = set()
        for doc_id, doc_rev, doc in docs_info:
            current_rev = self._get_current_rev(doc_id)
            seen_ids.add(doc_id)
            if VectorClockRev(doc_rev).is_newer(VectorClockRev(current_rev)):
                self._docs[doc_id] = (doc_rev, doc)
                self._transaction_log.append(doc_id)
            elif doc_rev == current_rev:
                # magical convergence
                continue
            else:
                conflict_ids.append(doc_id)
        return seen_ids, conflict_ids

    def _insert_conflicts(self, docs_info):
        for doc_id, doc_rev, doc in docs_info:
            my_doc_rev, my_doc = self._docs[doc_id]
            self._conflicts.setdefault(doc_id, []).append((my_doc_rev, my_doc))
            self._docs[doc_id] = (doc_rev, doc)
            self._transaction_log.append(doc_id)

    def sync_exchange(self, docs_info, from_machine_id, from_machine_rev,
                      last_known_rev):
        """Incorporate the documents sent from the other machine.

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
        seen_ids, conflict_ids = self._insert_many_docs(docs_info)
        new_docs = []
        for doc_id in self.whats_changed(last_known_rev):
            if doc_id in seen_ids:
                continue
            doc_rev, doc, _ = self.get_doc(doc_id)
            new_docs.append((doc_id, doc_rev, doc))
        self._other_revs[from_machine_id] = from_machine_rev
        conflicts = []
        for doc_id in conflict_ids:
            doc_rev, doc, _ = self.get_doc(doc_id)
            conflicts.append((doc_id, doc_rev, doc))
        self._last_exchange_log = {
            'receive': {'docs': [(di, dr) for di, dr, _ in docs_info],
                        'from_id': from_machine_id,
                        'from_rev': from_machine_rev,
                        'last_known_rev': last_known_rev},
            'return': {'new_docs': [(di, dr) for di, dr, _ in new_docs],
                       'conf_docs': [(di, dr) for di, dr, _ in conflicts],
                       'last_rev': len(self._transaction_log)}
        }
        return new_docs, conflicts, len(self._transaction_log)

    def sync(self, other, callback=None):
        (other_machine_id, other_rev,
         others_my_rev) = other.get_sync_info(self._machine_id)
        docs_to_send = []
        for doc_id in self.whats_changed(others_my_rev):
            doc_rev, doc, _ = self.get_doc(doc_id)
            docs_to_send.append((doc_id, doc_rev, doc))
        other_last_known_rev = self._other_revs.get(other_machine_id, 0)
        (new_records, conflicted_records,
         new_db_rev) = other.sync_exchange(docs_to_send, self._machine_id,
                            len(self._transaction_log),
                            other_last_known_rev)
        _, conflict_ids = self._insert_many_docs(new_records)
        self._insert_conflicts(conflicted_records)
        conflict_ids = set(conflict_ids)
        self._insert_conflicts([r for r in new_records if r[0] in conflict_ids])
        self.put_state_info(other_machine_id, new_db_rev)
        other.put_state_info(self._machine_id, len(self._transaction_log))


class InMemoryIndex(object):
    """Interface for managing an Index."""

    def __init__(self, index_name, index_definition):
        self._name = index_name
        self._definition = index_definition
        self._values = {}

    def evaluate_json(self, doc):
        """Determine the 'key' after applying this index to the doc."""
        raw = simplejson.loads(doc)
        return self.evaluate(raw)

    def evaluate(self, obj):
        """Evaluate a dict object, applying this definition."""
        result = []
        for field in self._definition:
            val = obj.get(field)
            if val is None:
                return None
            result.append(val)
        return '\x01'.join(result)

    def add_json(self, doc_id, doc):
        """Add this json doc to the index."""
        key = self.evaluate_json(doc)
        if key is None:
            return
        self._values.setdefault(key, []).append(doc_id)

    def remove_json(self, doc_id, doc):
        """Remove this json doc from the index."""
        key = self.evaluate_json(doc)
        doc_ids = self._values[key]
        doc_ids.remove(doc_id)
        if not doc_ids:
            del self._values[key]

    def lookup(self, values):
        """Find docs that match the values."""
        result = []
        for value in values:
            key = '\x01'.join(value)
            try:
                doc_ids = self._values[key]
            except KeyError:
                continue
            result.extend(doc_ids)
        return result


class VectorClockRev(object):

    def __init__(self, value):
        self._value = value

    def _expand(self):
        if not self._value:
            return {}
        result = {}
        for machine_info in self._value.split('|'):
            machine_id, counter = machine_info.split(':')
            counter = int(counter)
            result[machine_id] = counter
        return result

    def is_newer(self, other):
        """Is this VectorClockRev strictly newer than other.
        """
        if self._value is None:
            return False
        if other._value is None:
            return True
        this_expand = self._expand()
        other_expand = other._expand()
        this_is_newer = False
        for key, value in this_expand.iteritems():
            if key in other_expand:
                other_value = other_expand.pop(key)
                if other_value > value:
                    return False
                elif other_value < value:
                    this_is_newer = True
            else:
                this_is_newer = True
        if other_expand:
            return False
        return this_is_newer

    def increment(self, machine_id):
        """Increase the 'machine_id' section of this vector clock.

        :return: A string representing the new vector clock value
        """
        expanded = self._expand()
        expanded[machine_id] = expanded.get(machine_id, 0) + 1
        result = ['%s:%d' % (m, c) for m, c in sorted(expanded.items())]
        return '|'.join(result)
