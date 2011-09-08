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

"""The in-memory Database class for U1DB."""

import re
import simplejson

import u1db
from u1db.vectorclock import VectorClockRev


class InMemoryDatabase(u1db.Database):
    """A database that only stores the data internally."""

    def __init__(self, machine_id):
        self._transaction_log = []
        self._docs = {}
        # Map from doc_id => [(doc_rev, doc)] conflicts beyond 'winner'
        self._conflicts = {}
        self._other_revs = {}
        self._indexes = {}
        self._doc_counter = 0
        self._machine_id = machine_id
        self._last_exchange_log = None

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

    def create_doc(self, doc, doc_id=None):
        if doc_id is None:
            doc_id = self._allocate_doc_id()
        return doc_id, self.put_doc(doc_id, None, doc)

    def put_doc(self, doc_id, old_doc_rev, doc):
        if doc_id is None:
            raise u1db.InvalidDocId()
        old_doc = None
        if doc_id in self._docs:
            if doc_id in self._conflicts:
                raise u1db.ConflictedDoc()
            old_rev, old_doc = self._docs[doc_id]
            if old_rev != old_doc_rev:
                raise u1db.InvalidDocRev()
        new_rev = self._allocate_doc_rev(old_doc_rev)
        self._put_and_update_indexes(doc_id, old_doc, new_rev, doc)
        return new_rev

    def _put_and_update_indexes(self, doc_id, old_doc, new_rev, doc):
        for index in self._indexes.itervalues():
            if old_doc is not None:
                index.remove_json(doc_id, old_doc)
            if doc not in (None, 'null'):
                index.add_json(doc_id, doc)
        self._docs[doc_id] = (new_rev, doc)
        self._transaction_log.append(doc_id)

    def get_doc(self, doc_id):
        try:
            doc_rev, doc = self._docs[doc_id]
        except KeyError:
            return None, None, False
        if doc == 'null':
            doc = None
        return doc_rev, doc, (doc_id in self._conflicts)

    def get_doc_conflicts(self, doc_id):
        """Get the list of conflict texts for the given document.
        The order of the conflicts is such that the first entry is the value
        that would be returned by "GET_DOC".

        :return: [(doc_rev, doc)] a list of tuples of the revision for the
            content, and the JSON string of the content.
        """
        if doc_id not in self._conflicts:
            return []
        result = [self._docs[doc_id]]
        result.extend(self._conflicts[doc_id])
        return result

    def _get_current_rev(self, doc_id):
        return self._docs.get(doc_id, (None, None))[0]

    def resolve_doc(self, doc_id, doc, conflicted_doc_revs):
        cur_rev, cur_doc = self._docs[doc_id]
        vcr = VectorClockRev(cur_rev)
        for rev in conflicted_doc_revs:
            vcr = VectorClockRev(vcr.maximize(rev))
        new_rev = vcr.increment(self._machine_id)
        superseded_revs = set(conflicted_doc_revs)
        remaining_conflicts = []
        cur_conflicts = self._conflicts[doc_id]
        for c_rev, c_doc in cur_conflicts:
            if c_rev in superseded_revs:
                continue
            remaining_conflicts.append((c_rev, c_doc))
        if cur_rev in superseded_revs:
            self._put_and_update_indexes(doc_id, cur_doc, new_rev, doc)
        else:
            remaining_conflicts.append((new_rev, doc))
        if not remaining_conflicts:
            del self._conflicts[doc_id]
        else:
            self._conflicts[doc_id] = remaining_conflicts
        return new_rev, bool(remaining_conflicts)

    def delete_doc(self, doc_id, doc_rev):
        if doc_id not in self._docs:
            raise KeyError
        new_rev = self.put_doc(doc_id, doc_rev, None)
        return new_rev

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

    def whats_changed(self, old_db_rev=0):
        return (len(self._transaction_log),
                set(self._transaction_log[old_db_rev:]))

    def _insert_many_docs(self, docs_info):
        """Add a bunch of documents to the local store.

        This will only add entries if they supersede the local entries,
        otherwise the doc ids will be added to conflict_ids.
        :param docs_info: List of [(doc_id, doc_rev, doc)]
        :return: (seen_ids, conflict_ids) sets of entries that were seen, and
            what was considered conflicted and not added.
        """
        conflict_ids = set()
        seen_ids = set()
        for doc_id, doc_rev, doc in docs_info:
            cur_rev, cur_doc, _ = self.get_doc(doc_id)
            doc_vcr = VectorClockRev(doc_rev)
            cur_vcr = VectorClockRev(cur_rev)
            seen_ids.add(doc_id)
            if doc_vcr.is_newer(cur_vcr):
                self._put_and_update_indexes(doc_id, cur_doc, doc_rev, doc)
            elif doc_rev == cur_rev:
                # magical convergence
                continue
            elif cur_vcr.is_newer(doc_vcr):
                # Don't add this to seen_ids, because we have something newer,
                # so we should send it back, and we should not generate a
                # conflict
                seen_ids.remove(doc_id)
                continue
            else:
                conflict_ids.add(doc_id)
        return seen_ids, conflict_ids

    def _insert_conflicts(self, docs_info):
        for doc_id, doc_rev, doc in docs_info:
            my_doc_rev, my_doc = self._docs[doc_id]
            self._conflicts.setdefault(doc_id, []).append((my_doc_rev, my_doc))
            self._put_and_update_indexes(doc_id, my_doc, doc_rev, doc)

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
        for doc_id in self.whats_changed(last_known_rev)[1]:
            if doc_id in seen_ids:
                continue
            doc_rev, doc = self._docs[doc_id]
            new_docs.append((doc_id, doc_rev, doc))
        self._other_revs[from_machine_id] = from_machine_rev
        conflicts = []
        for doc_id in conflict_ids:
            doc_rev, doc = self._docs[doc_id]
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
        for doc_id in self.whats_changed(others_my_rev)[1]:
            doc_rev, doc = self._docs[doc_id]
            docs_to_send.append((doc_id, doc_rev, doc))
        other_last_known_rev = self._other_revs.get(other_machine_id, 0)
        (new_records, conflicted_records,
         new_db_rev) = other.sync_exchange(docs_to_send, self._machine_id,
                            len(self._transaction_log),
                            other_last_known_rev)
        all_records = new_records + conflicted_records
        _, conflict_ids = self._insert_many_docs(all_records)
        # self._insert_conflicts(conflicted_records)
        self._insert_conflicts([r for r in all_records if r[0] in conflict_ids])
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
