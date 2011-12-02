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

"""Code for parsing Index definitions."""

import string


class Getter(object):
    """Get a value from a document based on a specification."""

    def get(self, raw_doc):
        """Get a value from the document.

        :param raw_doc: a python dictionary to get the value from.
        :return: the value, possibly None
        """
        raise NotImplementedError(self.get)


class StaticGetter(Getter):
    """A getter that returns a defined value (independent of the doc)."""

    def __init__(self, value):
        """Create a StaticGetter.

        :param value: the value to return when get is called.
        """
        self.value = value

    def get(self, raw_doc):
        return self.value


class ExtractField(Getter):
    """Extract a field from the document."""

    def __init__(self, field):
        """Create an ExtractField object.

        When a document is passed to get() this will return a value
        from the document based on the field specifier passed to
        the constructor.

        None will be returned if the field is nonexistant, or refers to an
        object, rather than a simple type or list of simple types.

        :param field: a specifier for the field to return.
            This is either a field name, or a dotted field name.
        """
        self.field = field

    def get(self, raw_doc):
        for subfield in self.field.split('.'):
            if isinstance(raw_doc, dict):
                raw_doc = raw_doc.get(subfield)
            else:
                raw_doc = None
                break
        if isinstance(raw_doc, dict):
            raw_doc = None
        if isinstance(raw_doc, list):
            for val in raw_doc:
                if isinstance(val, dict) or isinstance(val, list):
                    raw_doc = None
                    break
        return raw_doc


class Transformation(Getter):
    """A transformation on a value from another Getter."""

    name = None
    """The name that the transform has in a query string."""

    def __init__(self, inner):
        """Create a transformation.

        :param inner: the Getter to transform the value for.
        """
        self.inner = inner

    def get(self, raw_doc):
        inner_value = self.inner.get(raw_doc)
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

    name = "lower"

    def _can_transform(self, val):
        if isinstance(val, int):
            return False
        if isinstance(val, bool):
            return False
        if isinstance(val, float):
            return False
        if isinstance(val, list):
            return False
        if isinstance(val, dict):
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

    name = "split_words"

    def _can_transform(self, val):
        if isinstance(val, int):
            return False
        if isinstance(val, bool):
            return False
        if isinstance(val, float):
            return False
        if isinstance(val, list):
            return False
        if isinstance(val, dict):
            return False
        return True

    def transform(self, value):
        if value is None:
            return value
        if isinstance(value, list):
            joined_values = []
            for val in value:
                if self._can_transform(val):
                    for word in val.split():
                        if word not in joined_values:
                            joined_values.append(word)
            return joined_values
        else:
            if self._can_transform(value):
                values = []
                for word in value.split():
                    if word not in values:
                        values.append(word)
                return values
            return None


class IsNull(Transformation):
    """Indicate whether the input is None.

    This Getter returns a bool indicating whether the input is nil.
    """

    name = "is_null"

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
    """Parse an index expression into a sequence of transformations."""

    _transformations = {}

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
        word, field = self._take_word(field)
        if field.startswith("("):
            # We have an operation
            if not field.endswith(")"):
                raise ParseError("Invalid transformation function: %s" % field)
            op = self._transformations.get(word, None)
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

    def parse_all(self, fields):
        return [self.parse(field) for field in fields]

    @classmethod
    def register_transormation(cls, transform):
        assert transform.name not in cls._transformations, (
                "Transform %s already registered for %s"
                % (transform.name, cls._transformations[transform.name]))
        cls._transformations[transform.name] = transform


Parser.register_transormation(SplitWords)
Parser.register_transormation(Lower)
Parser.register_transormation(IsNull)
