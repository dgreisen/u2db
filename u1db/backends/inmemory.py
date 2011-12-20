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

"""The in-memory Database class for U1DB."""

import simplejson

from u1db import (
    Document,
    errors,
    query_parser,
    vectorclock,
    )
from u1db.backends import CommonBackend, CommonSyncTarget


class InMemoryDatabase(CommonBackend):
    """A database that only stores the data internally."""

    def __init__(self, replica_uid):
        self._transaction_log = []
        self._docs = {}
        # Map from doc_id => [(doc_rev, doc)] conflicts beyond 'winner'
        self._conflicts = {}
        self._other_generations = {}
        self._indexes = {}
        self._replica_uid = replica_uid
        self._last_exchange_log = None

    def close(self):
        # This is a no-op, We don't want to free the data because one client
        # may be closing it, while another wants to inspect the results.
        pass

    def get_sync_generation(self, other_replica_uid):
        return self._other_generations.get(other_replica_uid, 0)

    def set_sync_generation(self, other_replica_uid, other_generation):
        self._set_sync_generation(other_replica_uid, other_generation)

    def _set_sync_generation(self, other_replica_uid, other_generation):
        # TODO: to handle race conditions, we may want to check if the current
        #       value is greater than this new value.
        self._other_generations[other_replica_uid] = other_generation

    def get_sync_target(self):
        return InMemorySyncTarget(self)

    def _get_transaction_log(self):
        # snapshot!
        return self._transaction_log[:]

    def _get_generation(self):
        return len(self._transaction_log)

    def put_doc(self, doc):
        if doc.doc_id is None:
            raise errors.InvalidDocId()
        self._check_doc_id(doc.doc_id)
        if self._has_conflicts(doc.doc_id):
            raise errors.ConflictedDoc()
        old_doc = self._get_doc(doc.doc_id)
        if old_doc is not None:
            if old_doc.rev != doc.rev:
                raise errors.RevisionConflict()
        else:
            if doc.rev is not None:
                raise errors.RevisionConflict()
        new_rev = self._allocate_doc_rev(doc.rev)
        doc.rev = new_rev
        self._put_and_update_indexes(old_doc, doc)
        return new_rev

    def _put_and_update_indexes(self, old_doc, doc):
        for index in self._indexes.itervalues():
            if old_doc is not None and old_doc.content is not None:
                index.remove_json(old_doc.doc_id, old_doc.content)
            if doc.content is not None:
                index.add_json(doc.doc_id, doc.content)
        self._docs[doc.doc_id] = (doc.rev, doc.content)
        self._transaction_log.append(doc.doc_id)

    def _get_doc(self, doc_id):
        try:
            doc_rev, content = self._docs[doc_id]
        except KeyError:
            return None
        return Document(doc_id, doc_rev, content)

    def _has_conflicts(self, doc_id):
        return doc_id in self._conflicts

    def get_doc(self, doc_id):
        doc = self._get_doc(doc_id)
        if doc is None:
            return None
        doc.has_conflicts = (doc.doc_id in self._conflicts)
        return doc

    def get_doc_conflicts(self, doc_id):
        if doc_id not in self._conflicts:
            return []
        result = [self._docs[doc_id]]
        result.extend(self._conflicts[doc_id])
        return result

    def _replace_conflicts(self, doc, conflicts):
        if not conflicts:
            del self._conflicts[doc.doc_id]
        else:
            self._conflicts[doc.doc_id] = conflicts
        doc.has_conflicts = bool(conflicts)

    def _prune_conflicts(self, doc, doc_vcr):
        if self._has_conflicts(doc.doc_id):
            remaining_conflicts = []
            cur_conflicts = self._conflicts[doc.doc_id]
            for c_rev, c_doc in cur_conflicts:
                if doc_vcr.is_newer(vectorclock.VectorClockRev(c_rev)):
                    continue
                remaining_conflicts.append((c_rev, c_doc))
            self._replace_conflicts(doc, remaining_conflicts)

    def resolve_doc(self, doc, conflicted_doc_revs):
        cur_doc = self._get_doc(doc.doc_id)
        if cur_doc is None:
            cur_rev = None
        else:
            cur_rev = cur_doc.rev
        new_rev = self._ensure_maximal_rev(cur_rev, conflicted_doc_revs)
        superseded_revs = set(conflicted_doc_revs)
        remaining_conflicts = []
        cur_conflicts = self._conflicts[doc.doc_id]
        for c_rev, c_doc in cur_conflicts:
            if c_rev in superseded_revs:
                continue
            remaining_conflicts.append((c_rev, c_doc))
        doc.rev = new_rev
        if cur_rev in superseded_revs:
            self._put_and_update_indexes(cur_doc, doc)
        else:
            remaining_conflicts.append((new_rev, doc.content))
        self._replace_conflicts(doc, remaining_conflicts)

    def delete_doc(self, doc):
        if doc.doc_id not in self._docs:
            raise errors.DocumentDoesNotExist
        if self._docs[doc.doc_id][1] in ('null', None):
            raise errors.DocumentAlreadyDeleted
        doc.content = None
        self.put_doc(doc)

    def create_index(self, index_name, index_expression):
        index = InMemoryIndex(index_name, index_expression)
        for doc_id, (doc_rev, doc) in self._docs.iteritems():
            index.add_json(doc_id, doc)
        self._indexes[index_name] = index

    def delete_index(self, index_name):
        del self._indexes[index_name]

    def list_indexes(self):
        definitions = []
        for idx in self._indexes.itervalues():
            definitions.append((idx._name, idx._definition))
        return definitions

    def get_from_index(self, index_name, key_values):
        index = self._indexes[index_name]
        doc_ids = index.lookup(key_values)
        result = []
        for doc_id in doc_ids:
            doc_rev, doc = self._docs[doc_id]
            result.append(Document(doc_id, doc_rev, doc))
        return result

    def whats_changed(self, old_generation=0):
        changes = []
        relevant_tail = self._transaction_log[old_generation:]
        cur_generation = old_generation + len(relevant_tail)
        seen = set()
        generation = cur_generation
        for doc_id in reversed(relevant_tail):
            if doc_id not in seen:
                changes.append((doc_id, generation))
                seen.add(doc_id)
            generation -= 1
        changes.reverse()
        return (cur_generation, changes)

    def _force_doc_sync_conflict(self, doc):
        my_doc = self._get_doc(doc.doc_id)
        self._prune_conflicts(doc, vectorclock.VectorClockRev(doc.rev))
        self._conflicts.setdefault(doc.doc_id, []).append(
            (my_doc.rev, my_doc.content))
        doc.has_conflicts = True
        self._put_and_update_indexes(my_doc, doc)


class InMemoryIndex(object):
    """Interface for managing an Index."""

    def __init__(self, index_name, index_definition):
        self._name = index_name
        self._definition = index_definition
        self._values = {}
        parser = query_parser.Parser()
        self._getters = parser.parse_all(self._definition)

    def evaluate_json(self, doc):
        """Determine the 'key' after applying this index to the doc."""
        raw = simplejson.loads(doc)
        return self.evaluate(raw)

    def evaluate(self, obj):
        """Evaluate a dict object, applying this definition."""
        all_rows = [[]]
        for getter in self._getters:
            new_rows = []
            keys = getter.get(obj)
            if not keys:
                return []
            for key in keys:
                new_rows.extend([row + [key] for row in all_rows])
            all_rows = new_rows
        all_rows = ['\x01'.join(row) for row in all_rows]
        return all_rows

    def add_json(self, doc_id, doc):
        """Add this json doc to the index."""
        keys = self.evaluate_json(doc)
        if not keys:
            return
        for key in keys:
            self._values.setdefault(key, []).append(doc_id)

    def remove_json(self, doc_id, doc):
        """Remove this json doc from the index."""
        keys = self.evaluate_json(doc)
        if keys:
            for key in keys:
                doc_ids = self._values[key]
                doc_ids.remove(doc_id)
                if not doc_ids:
                    del self._values[key]

    def _find_non_wildcards(self, values):
        """Check if this should be a wildcard match.

        Further, this will raise an exception if the syntax is improperly
        defined.

        :return: The offset of the last value we need to match against.
        """
        if len(values) != len(self._definition):
            raise errors.InvalidValueForIndex()
        is_wildcard = False
        last = 0
        for idx, val in enumerate(values):
            if val.endswith('*'):
                if val != '*':
                    # We have an 'x*' style wildcard
                    if is_wildcard:
                        # We were already in wildcard mode, so this is invalid
                        raise errors.InvalidValueForIndex()
                    last = idx + 1
                is_wildcard = True
            else:
                if is_wildcard:
                    # We were in wildcard mode, we can't follow that with
                    # non-wildcard
                    raise errors.InvalidValueForIndex()
                last = idx + 1
        if not is_wildcard:
            return -1
        return last

    def lookup(self, key_values):
        """Find docs that match the values."""
        result = []
        for values in key_values:
            last = self._find_non_wildcards(values)
            if last == -1:
                result.extend(self._lookup_exact(values))
            else:
                result.extend(self._lookup_prefix(values[:last]))
        return result

    def _lookup_prefix(self, value):
        """Find docs that match the prefix string in values."""
        # TODO: We need a different data structure to make prefix style fast,
        #       some sort of sorted list would work, but a plain dict doesn't.
        key_prefix = '\x01'.join(value)
        key_prefix = key_prefix.rstrip('*')
        all_doc_ids = []
        for key, doc_ids in self._values.iteritems():
            if key.startswith(key_prefix):
                all_doc_ids.extend(doc_ids)
        return all_doc_ids

    def _lookup_exact(self, value):
        """Find docs that match exactly."""
        key = '\x01'.join(value)
        if key in self._values:
            return self._values[key]
        return ()


class InMemorySyncTarget(CommonSyncTarget):

    def get_sync_info(self, source_replica_uid):
        source_gen = self._db.get_sync_generation(source_replica_uid)
        return self._db._replica_uid, len(self._db._transaction_log), source_gen

    def record_sync_info(self, source_replica_uid, source_replica_generation):
        self._db.set_sync_generation(source_replica_uid,
                                     source_replica_generation)
