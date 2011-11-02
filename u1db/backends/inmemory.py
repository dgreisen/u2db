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
import string

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


class Getter(object):
    """Get a value from a document based on a specification."""

    def get(self, doc):
        """Get a value from the document.

        :param doc: the doc to get the value from.
        :return: the value, possibly None
        """
        raise NotImplementedError(self.get)


class StaticGetter(Getter):
    """A getter that returns a defined value."""

    def __init__(self, value):
        """Create a StaticGetter.

        :param value: the value to return when get is called.
        """
        self.value = value

    def get(self, value):
        return self.value


class ExtractField(Getter):
    """Extract a field from the document."""

    def __init__(self, field):
        """Create an ExtractField object.

        When a document is passed to get() this will return a value
        from the docuemnt based on the field specifier passed to
        the constructor.

        If the field specifier refers to a field in the document
        that is not present, then None will be returned.

        :param field: a specifier for the field to return.
            This is either a field name, or a dotted field name.
        """
        self.field = field

    def get(self, value):
        for subfield in self.field.split('.'):
            if isinstance(value, dict):
                value = value.get(subfield)
            else:
                value = None
                break
        if isinstance(value, dict):
            value = None
        if isinstance(value, list):
            for val in value:
                if isinstance(val, dict) or isinstance(val, list):
                    value = None
                    break
        return value


class Transformation(Getter):
    """A transformation on a value from another Getter."""

    def __init__(self, inner):
        """Create a transformation.

        :param inner: the Getter to transform the value for.
        """
        self.inner = inner

    def get(self, value):
        inner_value = self.inner.get(value)
        return self.transform(inner_value)

    def transform(self, value):
        """Transform the value.

        This should be implemented by subclasses to transform the
        value when get() is called.

        :param value: the value from the other Getter. May be None.
        :return: the transformed value.
        """
        raise NotImplementedError(self.transform)


class Lower(Transformation):
    """Lowercase a string.

    This transformation will return None for non-string inputs. However,
    it will lowercase any strings in a list, dropping any elements
    that are not strings.
    """

    def _can_transform(self, val):
        if isinstance(val, int):
            return False
        if isinstance(val, bool):
            return False
        if isinstance(val, float):
            return False
        return True

    def transform(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            return [val.lower() for val in value if self._can_transform(val)]
        else:
            if self._can_transform(value):
                return value.lower()
            return None


class SplitWords(Transformation):
    """Split a string on whitespace.

    This Getter will return None for non-string inputs. It will however
    split any strings in an input list, discarding any elements that
    are not strings.
    """

    def _can_transform(self, val):
        if isinstance(val, int):
            return False
        if isinstance(val, bool):
            return False
        if isinstance(val, float):
            return False
        return True

    def transform(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            joined_values = []
            for val in value:
                if self._can_transform(val):
                    # XXX: de-duplicate?
                    joined_values.extend(val.split())
            return joined_values
        else:
            if self._can_transform(value):
                return value.split()
            return None


class IsNull(Transformation):
    """Indicate whether the input is None.

    This Getter returns a bool indicating whether the input is nil.
    """

    def transform(self, value):
        return value is None


class EnsureListTransformation(Transformation):
    """A Getter than ensures a list is returned.

    Unless the input is None, the output will be a list. If the input
    is a list then it is returned unchanged, otherwise the input is
    made the only element of the returned list.
    """

    def transform(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            return value
        return [value]


class ParseError(Exception):
    pass


class Parser(object):

    OPERATIONS = {
        "lower": Lower,
        "split_words": SplitWords,
        "is_null": IsNull,
        }

    def _take_word(self, partial):
        i = 0
        word = ""
        while i < len(partial):
            char = partial[i]
            if char in string.lowercase + string.uppercase + "._" + string.digits:
                word += char
                i += 1
            else:
                break
        return word, partial[i:]

    def parse(self, field):
        inner = self._inner_parse(field)
        inner = EnsureListTransformation(inner)
        return inner

    def _inner_parse(self, field):
        # XXX: crappy parser
        word, field = self._take_word(field)
        if field.startswith("("):
            # We have an operation
            if not field.endswith(")"):
                raise ParseError("Invalid transformation function: %s" % field)
            op = self.OPERATIONS.get(word, None)
            if op is None:
                raise AssertionError("Unknown operation: %s" % word)
            inner = self._inner_parse(field[1:-1])
            return op(inner)
        else:
            assert len(field) == 0, "Unparsed chars: %s" % field
            if len(word) <= 0:
                raise ParseError("Missing field specifier")
            if word.endswith("."):
                raise ParseError("Invalid field specifier: %s" % word)
            return ExtractField(word)


class InMemoryIndex(object):
    """Interface for managing an Index."""

    def __init__(self, index_name, index_definition):
        self._name = index_name
        self._definition = index_definition
        self._values = {}
        parser = Parser()
        self._getters = []
        for field in self._definition:
            getter = parser.parse(field)
            self._getters.append(getter)

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
            if keys is None:
                return None
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

    def get_sync_info(self, other_replica_uid):
        other_gen = self._db.get_sync_generation(other_replica_uid)
        return self._db._replica_uid, len(self._db._transaction_log), other_gen

    def record_sync_info(self, other_replica_uid, other_replica_generation):
        self._db.set_sync_generation(other_replica_uid,
                                     other_replica_generation)

