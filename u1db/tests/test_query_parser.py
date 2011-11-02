from u1db import (
    query_parser,
    tests,
    )


class ExtractFieldTests(tests.TestCase):

    def test_get_value(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo="bar"))
        self.assertEqual("bar", val)

    def test_get_value_None(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=None))
        self.assertEqual(None, val)

    def test_get_value_missing_key(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict())
        self.assertEqual(None, val)

    def test_get_value_subfield(self):
        getter = query_parser.ExtractField('foo.baz')
        val = getter.get(dict(foo=dict(baz="bar")))
        self.assertEqual("bar", val)

    def test_get_value_subfield_missing(self):
        getter = query_parser.ExtractField('foo.baz')
        val = getter.get(dict(foo="bar"))
        self.assertEqual(None, val)

    def test_get_value_dict(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=dict(baz="bar")))
        # XXX: should give None or error?
        self.assertEqual(None, val)

    def test_get_value_list(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=["bar", "zap"]))
        self.assertEqual(["bar", "zap"], val)

    def test_get_value_list_of_dicts(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=[dict(zap="bar")]))
        self.assertEqual(None, val)

    def test_get_value_int(self):
        # XXX: is the API defined to store and return numbers as
        # numbers as strings?
        # i.e. do we need a "number" transformation?
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=9))
        self.assertEqual(9, val)

    def test_get_value_float(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=9.0))
        self.assertEqual(9.0, val)

    def test_get_value_bool(self):
        getter = query_parser.ExtractField('foo')
        val = getter.get(dict(foo=True))
        self.assertEqual(True, val)


class LowerTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = query_parser.Lower(query_parser.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(None, val)

    def test_inner_returns_string(self):
        getter = query_parser.Lower(query_parser.StaticGetter('fOo'))
        val = getter.get('bar')
        self.assertEqual('foo', val)

    def test_inner_returns_list(self):
        getter = query_parser.Lower(query_parser.StaticGetter(['fOo', 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_int(self):
        getter = query_parser.Lower(query_parser.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_float(self):
        getter = query_parser.Lower(query_parser.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_bool(self):
        getter = query_parser.Lower(query_parser.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_list_containing_int(self):
        getter = query_parser.Lower(query_parser.StaticGetter(['fOo', 9, 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list_containing_float(self):
        getter = query_parser.Lower(query_parser.StaticGetter(['fOo', 9.0, 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list_containing_bool(self):
        getter = query_parser.Lower(query_parser.StaticGetter(['fOo', True, 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list_containing_list(self):
        getter = query_parser.Lower(query_parser.StaticGetter(['fOo', ['bAa'], 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list_containing_dict(self):
        getter = query_parser.Lower(query_parser.StaticGetter(['fOo', dict(baa="xam"), 'bAr']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)


class SplitWordsTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = query_parser.SplitWords(query_parser.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(None, val)

    def test_inner_returns_string(self):
        getter = query_parser.SplitWords(query_parser.StaticGetter('foo bar'))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_list(self):
        getter = query_parser.SplitWords(
            query_parser.StaticGetter(['foo baz', 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_int(self):
        getter = query_parser.SplitWords(query_parser.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_float(self):
        getter = query_parser.SplitWords(query_parser.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_bool(self):
        getter = query_parser.SplitWords(query_parser.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual(None, val)

    def test_inner_returns_list_containing_int(self):
        getter = query_parser.SplitWords(
            query_parser.StaticGetter(['foo baz', 9, 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_list_containing_float(self):
        getter = query_parser.SplitWords(
            query_parser.StaticGetter(['foo baz', 9.0, 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_list_containing_bool(self):
        getter = query_parser.SplitWords(
            query_parser.StaticGetter(['foo baz', True, 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_list_containing_list(self):
        getter = query_parser.SplitWords(
            query_parser.StaticGetter(['foo baz', ["baa"], 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)

    def test_inner_returns_list_containing_dict(self):
        getter = query_parser.SplitWords(
            query_parser.StaticGetter(['foo baz', dict(baa="xam"), 'bar sux']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'baz','bar', 'sux'], val)


class IsNullTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = query_parser.IsNull(query_parser.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(True, val)

    def test_inner_returns_string(self):
        getter = query_parser.IsNull(query_parser.StaticGetter('foo'))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_list(self):
        getter = query_parser.IsNull(query_parser.StaticGetter(['foo', 'bar']))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_int(self):
        getter = query_parser.IsNull(query_parser.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_float(self):
        getter = query_parser.IsNull(query_parser.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual(False, val)

    def test_inner_returns_bool(self):
        getter = query_parser.IsNull(query_parser.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual(False, val)


class EnsureListTransformationTests(tests.TestCase):

    def test_inner_returns_None(self):
        getter = query_parser.EnsureListTransformation(
                query_parser.StaticGetter(None))
        val = getter.get('foo')
        self.assertEqual(None, val)

    def test_inner_returns_string(self):
        getter = query_parser.EnsureListTransformation(
                query_parser.StaticGetter('foo'))
        val = getter.get('zap')
        self.assertEqual(['foo'], val)

    def test_inner_returns_list(self):
        getter = query_parser.EnsureListTransformation(
                query_parser.StaticGetter(['foo', 'bar']))
        val = getter.get('zap')
        self.assertEqual(['foo', 'bar'], val)

    def test_inner_returns_int(self):
        getter = query_parser.EnsureListTransformation(
                query_parser.StaticGetter(9))
        val = getter.get('zap')
        self.assertEqual([9], val)

    def test_inner_returns_float(self):
        getter = query_parser.EnsureListTransformation(
                query_parser.StaticGetter(9.0))
        val = getter.get('zap')
        self.assertEqual([9.0], val)

    def test_inner_returns_bool(self):
        getter = query_parser.EnsureListTransformation(
                query_parser.StaticGetter(True))
        val = getter.get('zap')
        self.assertEqual([True], val)


class ParserTests(tests.TestCase):

    def parse(self, spec):
        parser = query_parser.Parser()
        return parser.parse(spec)

    def parse_all(self, specs):
        parser = query_parser.Parser()
        return parser.parse_all(specs)

    def test_parse_empty_string(self):
        self.assertRaises(query_parser.ParseError, self.parse, "")

    def test_parse_field(self):
        getter = self.parse("a")
        self.assertIsInstance(getter, query_parser.EnsureListTransformation)
        self.assertIsInstance(getter.inner, query_parser.ExtractField)
        self.assertEqual("a", getter.inner.field)

    def test_parse_dotted_field(self):
        getter = self.parse("a.b")
        self.assertIsInstance(getter, query_parser.EnsureListTransformation)
        self.assertIsInstance(getter.inner, query_parser.ExtractField)
        self.assertEqual("a.b", getter.inner.field)

    def test_parse_dotted_field_nothing_after_dot(self):
        self.assertRaises(query_parser.ParseError, self.parse, "a.")

    def test_parse_missing_close_on_transformation(self):
        self.assertRaises(query_parser.ParseError, self.parse, "lower(a")

    def test_parse_missing_field_in_transformation(self):
        self.assertRaises(query_parser.ParseError, self.parse, "lower()")

    def test_parse_transformation(self):
        getter = self.parse("lower(a)")
        self.assertIsInstance(getter, query_parser.EnsureListTransformation)
        self.assertIsInstance(getter.inner, query_parser.Lower)
        self.assertIsInstance(getter.inner.inner, query_parser.ExtractField)
        self.assertEqual("a", getter.inner.inner.field)

    def test_parse_all(self):
        getters = self.parse_all(["a", "b"])
        self.assertEqual(2, len(getters))
        self.assertIsInstance(getters[0], query_parser.EnsureListTransformation)
        self.assertIsInstance(getters[0].inner, query_parser.ExtractField)
        self.assertEqual("a", getters[0].inner.field)
        self.assertIsInstance(getters[1], query_parser.EnsureListTransformation)
        self.assertIsInstance(getters[1].inner, query_parser.ExtractField)
        self.assertEqual("b", getters[1].inner.field)
