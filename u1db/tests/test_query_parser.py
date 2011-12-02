from u1db import (
    query_parser,
    tests,
    )


trivial_raw_doc = {}

class TestStaticGetter(tests.TestCase):

    def test_returns_string(self):
        getter = query_parser.StaticGetter('foo')
        self.assertEqual('foo', getter.get(trivial_raw_doc))

    def test_returns_int(self):
        getter = query_parser.StaticGetter(9)
        self.assertEqual(9, getter.get(trivial_raw_doc))

    def test_returns_float(self):
        getter = query_parser.StaticGetter(9.2)
        self.assertEqual(9.2, getter.get(trivial_raw_doc))

    def test_returns_None(self):
        getter = query_parser.StaticGetter(None)
        self.assertIs(None, getter.get(trivial_raw_doc))


class TestExtractField(tests.TestCase):

    def assertExtractField(self, expected, field_name, raw_doc):
        getter = query_parser.ExtractField(field_name)
        self.assertEqual(expected, getter.get(raw_doc))

    def test_get_value(self):
        self.assertExtractField('bar', 'foo', {'foo': 'bar'})

    def test_get_value_None(self):
        self.assertExtractField(None, 'foo', {'foo': None})

    def test_get_value_missing_key(self):
        self.assertExtractField(None, 'foo', {})

    def test_get_value_subfield(self):
        self.assertExtractField('bar', 'foo.baz', {'foo': {'baz': 'bar'}})

    def test_get_value_subfield_missing(self):
        self.assertExtractField(None, 'foo.baz', {'foo': 'bar'})

    def test_get_value_dict(self):
        self.assertExtractField(None, 'foo', {'foo': {'baz': 'bar'}})

    def test_get_value_list(self):
        self.assertExtractField(['bar', 'zap'], 'foo', {'foo': ['bar', 'zap']})

    def test_get_value_list_of_dicts(self):
        self.assertExtractField(None, 'foo', {'foo': [{'zap': 'bar'}]})

    def test_get_value_int(self):
        self.assertExtractField(9, 'foo', {'foo': 9})

    def test_get_value_float(self):
        self.assertExtractField(9.2, 'foo', {'foo': 9.2})

    def test_get_value_bool(self):
        self.assertExtractField(True, 'foo', {'foo': True})
        self.assertExtractField(False, 'foo', {'foo': False})


class TestLower(tests.TestCase):

    def assertLowerGets(self, expected, input_val):
        getter = query_parser.Lower(query_parser.StaticGetter(input_val))
        out_val = getter.get(trivial_raw_doc)
        self.assertEqual(expected, out_val)

    def test_inner_returns_None(self):
        self.assertLowerGets(None, None)

    def test_inner_returns_string(self):
        self.assertLowerGets('foo', 'fOo')

    def test_inner_returns_list(self):
        self.assertLowerGets(['foo', 'bar'], ['fOo', 'bAr'])

    def test_inner_returns_int(self):
        self.assertLowerGets(None, 9)

    def test_inner_returns_float(self):
        self.assertLowerGets(None, 9.0)

    def test_inner_returns_bool(self):
        self.assertLowerGets(None, True)

    def test_inner_returns_list_containing_int(self):
        self.assertLowerGets(['foo', 'bar'], ['fOo', 9, 'bAr'])

    def test_inner_returns_list_containing_float(self):
        self.assertLowerGets(['foo', 'bar'], ['fOo', 9.2, 'bAr'])

    def test_inner_returns_list_containing_bool(self):
        self.assertLowerGets(['foo', 'bar'], ['fOo', True, 'bAr'])

    def test_inner_returns_list_containing_list(self):
        # TODO: Should this be unfolding the inner list?
        self.assertLowerGets(['foo', 'bar'], ['fOo', ['bAa'], 'bAr'])

    def test_inner_returns_list_containing_dict(self):
        self.assertLowerGets(['foo', 'bar'], ['fOo', {'baa': 'xam'}, 'bAr'])


class TestSplitWords(tests.TestCase):

    def assertSplitWords(self, expected, value):
        getter = query_parser.SplitWords(query_parser.StaticGetter(value))
        self.assertEqual(expected, getter.get(trivial_raw_doc))

    def test_inner_returns_None(self):
        self.assertSplitWords(None, None)

    def test_inner_returns_string(self):
        self.assertSplitWords(['foo', 'bar'], 'foo bar')

    def test_inner_returns_list(self):
        self.assertSplitWords(['foo', 'baz', 'bar', 'sux'],
                              ['foo baz', 'bar sux'])

    def test_inner_returns_int(self):
        self.assertSplitWords(None, 9)

    def test_inner_returns_float(self):
        self.assertSplitWords(None, 9.2)

    def test_inner_returns_bool(self):
        self.assertSplitWords(None, True)

    def test_inner_returns_list_containing_int(self):
        self.assertSplitWords(['foo', 'baz', 'bar', 'sux'],
                              ['foo baz', 9, 'bar sux'])

    def test_inner_returns_list_containing_float(self):
        self.assertSplitWords(['foo', 'baz', 'bar', 'sux'],
                              ['foo baz', 9.2, 'bar sux'])

    def test_inner_returns_list_containing_bool(self):
        self.assertSplitWords(['foo', 'baz', 'bar', 'sux'],
                              ['foo baz', True, 'bar sux'])

    def test_inner_returns_list_containing_list(self):
        # TODO: Expand sub-lists?
        self.assertSplitWords(['foo', 'baz', 'bar', 'sux'],
                              ['foo baz', ['baa'], 'bar sux'])

    def test_inner_returns_list_containing_dict(self):
        self.assertSplitWords(['foo', 'baz', 'bar', 'sux'],
                              ['foo baz', {'baa': 'xam'}, 'bar sux'])


class TestIsNull(tests.TestCase):

    def assertIsNull(self, value):
        getter = query_parser.IsNull(query_parser.StaticGetter(value))
        self.assertEqual(True, getter.get(trivial_raw_doc))

    def assertIsNotNull(self, value):
        getter = query_parser.IsNull(query_parser.StaticGetter(value))
        self.assertEqual(False, getter.get(trivial_raw_doc))

    def test_inner_returns_None(self):
        self.assertIsNull(None)

    def test_inner_returns_string(self):
        self.assertIsNotNull('foo')

    def test_inner_returns_list(self):
        self.assertIsNotNull(['foo', 'bar'])

    def test_inner_returns_int(self):
        self.assertIsNotNull(9)

    def test_inner_returns_float(self):
        self.assertIsNotNull(9.2)

    def test_inner_returns_bool(self):
        self.assertIsNotNull(True)

    # TODO: What about a dict? Inner is likely to return None, even though the
    #       attribute does exist...


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
