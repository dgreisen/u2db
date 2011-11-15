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

from u1db import errors
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
        # TODO: to handle race conditions, we may want to check if the current
        #       value is greater than this new value.
        self._other_generations[other_replica_uid] = other_generation

    def get_sync_target(self):
        return InMemorySyncTarget(self)

    def _allocate_doc_id(self):
        return 'doc-%d' % (len(self._transaction_log) + 1,)

    def _get_transaction_log(self):
        return self._transaction_log

    def _get_generation(self):
        return len(self._transaction_log)

    def put_doc(self, doc_id, old_doc_rev, doc):
        if doc_id is None:
            raise errors.InvalidDocId()
        old_doc = None
        if doc_id in self._docs:
            if doc_id in self._conflicts:
                raise errors.ConflictedDoc()
            old_rev, old_doc = self._docs[doc_id]
            if old_rev != old_doc_rev:
                raise errors.InvalidDocRev()
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

    def _get_doc(self, doc_id):
        try:
            doc_rev, doc = self._docs[doc_id]
        except KeyError:
            return None, None
        return doc_rev, doc

    def _has_conflicts(self, doc_id):
        return doc_id in self._conflicts

    def get_doc(self, doc_id):
        doc_rev, doc = self._get_doc(doc_id)
        if doc == 'null':
            doc = None
        return doc_rev, doc, (doc_id in self._conflicts)

    def get_doc_conflicts(self, doc_id):
        if doc_id not in self._conflicts:
            return []
        result = [self._docs[doc_id]]
        result.extend(self._conflicts[doc_id])
        return result

    def resolve_doc(self, doc_id, doc, conflicted_doc_revs):
        cur_rev, cur_doc = self._docs[doc_id]
        new_rev = self._ensure_maximal_rev(cur_rev, conflicted_doc_revs)
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
        if self._docs[doc_id][1] in ('null', None):
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
            result.append((doc_id, doc_rev, doc))
        return result

    def whats_changed(self, old_generation=0):
        return (len(self._transaction_log),
                set(self._transaction_log[old_generation:]))

    def force_doc_sync_conflict(self, doc_id, doc_rev, doc):
        my_doc_rev, my_doc = self._docs[doc_id]
        self._conflicts.setdefault(doc_id, []).append((my_doc_rev, my_doc))
        self._put_and_update_indexes(doc_id, my_doc, doc_rev, doc)


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
            val = obj
            for subfield in field.split('.'):
                val = val.get(subfield)
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
        if key is None:
            return
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

    def get_sync_info(self, other_replica_uid):
        other_gen = self._db.get_sync_generation(other_replica_uid)
        return self._db._replica_uid, len(self._db._transaction_log), other_gen

    def record_sync_info(self, other_replica_uid, other_replica_generation):
        self._db.set_sync_generation(other_replica_uid,
                                     other_replica_generation)

