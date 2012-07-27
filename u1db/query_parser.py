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

"""Code for parsing Index definitions."""

import re
from u1db import (
    errors,
    )


class Getter(object):
    """Get values from a document based on a specification."""

    def get(self, raw_doc):
        """Get a value from the document.

        :param raw_doc: a python dictionary to get the value from.
        :return: A list of values that match the description.
        """
        raise NotImplementedError(self.get)


class StaticGetter(Getter):
    """A getter that returns a defined value (independent of the doc)."""

    def __init__(self, value):
        """Create a StaticGetter.

        :param value: the value to return when get is called.
        """
        if value is None:
            self.value = []
        elif isinstance(value, list):
            self.value = value
        else:
            self.value = [value]

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
                return []
        if isinstance(raw_doc, dict):
            return []
        if raw_doc is None:
            result = []
        elif isinstance(raw_doc, list):
            # Strip anything in the list that isn't a simple type
            result = [val for val in raw_doc
                      if not isinstance(val, (dict, list))]
        else:
            result = [raw_doc]
        return result


class Transformation(Getter):
    """A transformation on a value from another Getter."""

    name = None
    arity = 1
    args = ['expression']

    def __init__(self, inner):
        """Create a transformation.

        :param inner: the argument(s) to the transformation.
        """
        self.inner = inner

    def get(self, raw_doc):
        inner_values = self.inner.get(raw_doc)
        assert isinstance(inner_values, list),\
            'get() should always return a list'
        return self.transform(inner_values)

    def transform(self, values):
        """Transform the values.

        This should be implemented by subclasses to transform the
        value when get() is called.

        :param values: the values from the other Getter
        :return: the transformed values.
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
        return isinstance(val, basestring)

    def transform(self, values):
        if not values:
            return []
        return [val.lower() for val in values if self._can_transform(val)]


class Number(Transformation):
    """Convert an integer to a zero padded string.

    This transformation will return None for non-integer inputs. However, it
    will transform any integers in a list, dropping any elements that are not
    integers.
    """

    name = 'number'
    arity = 2
    args = ['expression', int]

    def __init__(self, inner, number):
        super(Number, self).__init__(inner)
        self.padding = "%%0%sd" % number

    def _can_transform(self, val):
        return isinstance(val, int) and not isinstance(val, bool)

    def transform(self, values):
        """Transform any integers in values into zero padded strings."""
        if not values:
            return []
        return [self.padding % (v,) for v in values if self._can_transform(v)]


class Bool(Transformation):
    """Convert bool to string."""

    name = "bool"
    args = ['expression']

    def _can_transform(self, val):
        return isinstance(val, bool)

    def transform(self, values):
        """Transform any booleans in values into strings."""
        if not values:
            return []
        return [('1' if v else '0') for v in values if self._can_transform(v)]


class SplitWords(Transformation):
    """Split a string on whitespace.

    This Getter will return [] for non-string inputs. It will however
    split any strings in an input list, discarding any elements that
    are not strings.
    """

    name = "split_words"

    def _can_transform(self, val):
        return isinstance(val, basestring)

    def transform(self, values):
        if not values:
            return []
        result = set()
        for value in values:
            if self._can_transform(value):
                for word in value.split():
                    result.add(word)
        return list(result)


class Combine(Transformation):
    """Combine multiple expressions into a single index."""

    name = "combine"
    # variable number of args
    arity = -1

    def __init__(self, *inner):
        super(Combine, self).__init__(inner)

    def get(self, raw_doc):
        inner_values = []
        for inner in self.inner:
            inner_values.extend(inner.get(raw_doc))
        return self.transform(inner_values)

    def transform(self, values):
        return values


class IsNull(Transformation):
    """Indicate whether the input is None.

    This Getter returns a bool indicating whether the input is nil.
    """

    name = "is_null"

    def transform(self, values):
        return [len(values) == 0]


def check_fieldname(fieldname):
    if fieldname.endswith('.'):
        raise errors.IndexDefinitionParseError(
            "Fieldname cannot end in '.':%s^" % (fieldname,))


class Parser(object):
    """Parse an index expression into a sequence of transformations."""

    _transformations = {}
    _delimiters = re.compile("\(|\)|,")

    def __init__(self):
        self._open_parens = 0
        self._tokens = []

    def _set_expression(self, expression):
        self._open_parens = 0
        self._tokens = []
        expression = expression.strip()
        while expression:
            delimiter = self._delimiters.search(expression)
            if delimiter:
                idx = delimiter.start()
                if idx == 0:
                    result, expression = (expression[:1], expression[1:])
                    self._tokens.append(result)
                else:
                    result, expression = (expression[:idx], expression[idx:])
                    result = result.strip()
                    if result:
                        self._tokens.append(result)
            else:
                expression = expression.strip()
                if expression:
                    self._tokens.append(expression)
                expression = None

    def _get_token(self):
        if self._tokens:
            return self._tokens.pop(0)

    def _peek(self):
        if self._tokens:
            return self._tokens[0]

    def _make_op(self, op_name):
        op = self._transformations.get(op_name, None)
        if op is None:
            raise errors.IndexDefinitionParseError(
                "Unknown operation: %s" % op_name)
        args = self._make_tree()
        if op.arity >= 0 and len(args) != op.arity:
            raise errors.IndexDefinitionParseError(
                "Invalid number of arguments for transformation "
                "function: %s, %r" % (op_name, args))
        parsed = []
        for i, arg in enumerate(args):
            arg_type = op.args[i % len(op.args)]
            if arg_type == 'expression':
                if isinstance(arg, Getter):
                    inner = arg
                else:
                    check_fieldname(arg)
                    inner = ExtractField(arg)
            else:
                try:
                    inner = arg_type(arg)
                except ValueError, e:
                    raise errors.IndexDefinitionParseError(
                        "Invalid value %r for argument type %r "
                        "(%r)." % (arg, arg_type, e))
            parsed.append(inner)
        return op(*parsed)

    def _make_tree(self):
        tree = []
        while True:
            token = self._get_token()
            if token in ('(', ',', ')'):
                raise errors.IndexDefinitionParseError(
                    "Encountered delimiter as first character of expression.")
            next_token = self._peek()
            if not next_token:
                if token:
                    check_fieldname(token)
                    tree.append(ExtractField(token))
                return tree
            if next_token == '(':
                self._open_parens += 1
                op_name = token
                self._get_token()
                op = self._make_op(op_name)
                tree.append(op)
            elif next_token == ')':
                self._open_parens -= 1
                if self._open_parens < 0:
                    raise errors.IndexDefinitionParseError(
                        "Encountered ')' before '('")
                term = token
                if term:
                    tree.append(term)
                self._get_token()
                while self._peek() == ')':
                    self._get_token()
                    self._open_parens -= 1
                    if self._open_parens < 0:
                        raise errors.IndexDefinitionParseError(
                            "Encountered ')' before '('")
                return tree
            elif next_token == ',':
                if self._open_parens < 1:
                    raise errors.IndexDefinitionParseError(
                        "Encountered ',' outside parentheses")
                term = token
                if term:
                    tree.append(term)
                self._get_token()

    def parse(self, expression):
        self._set_expression(expression)
        tree = self._make_tree()
        if self._open_parens:
            raise errors.IndexDefinitionParseError(
                "%d missing ')'s when parsing '%s'." %
                (self._open_parens, expression))
        if len(tree) != 1:
            raise errors.IndexDefinitionParseError(
                "Invalid expression: %s" % expression)
        return tree[0]

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
Parser.register_transormation(Number)
Parser.register_transormation(Bool)
Parser.register_transormation(IsNull)
Parser.register_transormation(Combine)
