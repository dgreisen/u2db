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

"""Tests for the buffering classes"""

import cStringIO

from u1db import (
    buffers,
    tests,
    )


class TestBuffer(tests.TestCase):

    def setUp(self):
        super(TestBuffer, self).setUp()
        self.buf = buffers.Buffer()

    def test_add_bytes(self):
        self.buf.add_bytes('abc')
        self.assertEqual(['abc'], self.buf._content)
        self.assertEqual(3, len(self.buf))
        self.buf.add_bytes('def')
        self.assertEqual(['abc', 'def'], self.buf._content)
        self.assertEqual(6, len(self.buf))

    def test_peek_bytes(self):
        self.buf.add_bytes('abc')
        self.buf.add_bytes('def')
        self.assertEqual(['abc', 'def'], self.buf._content)
        # If we can peek without combining, do so
        self.assertEqual('ab', self.buf.peek_bytes(2))
        self.assertEqual(['abc', 'def'], self.buf._content)
        self.assertEqual('abc', self.buf.peek_bytes(3))
        self.assertEqual(['abc', 'def'], self.buf._content)
        # After a bigger peek, we combine and save the larger string
        self.assertEqual('abcd', self.buf.peek_bytes(4))
        self.assertEqual(['abcdef'], self.buf._content)

    def test_peek_bytes_no_bytes(self):
        self.assertEqual(None, self.buf.peek_bytes(2))

    def test_peek_bytes_not_enough_bytes(self):
        self.buf.add_bytes('abc')
        self.buf.add_bytes('def')
        self.assertEqual(None, self.buf.peek_bytes(8))
        self.assertEqual(['abc', 'def'], self.buf._content)

    def test_peek_line_exact(self):
        content = 'ab\n'
        self.buf.add_bytes(content)
        self.assertEqual(['ab\n'], self.buf._content)
        self.assertEqual('ab\n', self.buf.peek_line())
        self.assertIs(content, self.buf.peek_line())

    def test_peek_line_multiple_chunks(self):
        self.buf.add_bytes('ab')
        self.buf.add_bytes('cd\n')
        self.assertEqual(['ab', 'cd\n'], self.buf._content)
        self.assertEqual('abcd\n', self.buf.peek_line())
        self.assertEqual(['abcd\n'], self.buf._content)

    def test_peek_line_partial_chunk(self):
        self.buf.add_bytes('ab\ncd')
        self.buf.add_bytes('ef')
        self.assertEqual(['ab\ncd', 'ef'], self.buf._content)
        self.assertEqual('ab\n', self.buf.peek_line())
        self.assertEqual(['ab\ncd', 'ef'], self.buf._content)

    def test_peek_line_mixed(self):
        self.buf.add_bytes('ab')
        self.buf.add_bytes('cd\nef')
        self.assertEqual(['ab', 'cd\nef'], self.buf._content)
        self.assertEqual('abcd\n', self.buf.peek_line())
        self.assertEqual(['abcd\nef'], self.buf._content)

    def test_peek_line_no_bytes(self):
        self.assertIs(None, self.buf.peek_line())

    def test_peek_no_line(self):
        self.buf.add_bytes('ab')
        self.buf.add_bytes('cd')
        self.assertEqual(['ab', 'cd'], self.buf._content)
        self.assertIs(None, self.buf.peek_line())
        self.assertEqual(['abcd'], self.buf._content)

    def test_consume_bytes(self):
        start = 'ab\n'
        self.buf.add_bytes(start)
        self.buf.add_bytes('cd')
        self.buf.add_bytes('ef')
        self.assertEqual(['ab\n', 'cd', 'ef'], self.buf._content)
        self.assertEqual(7, len(self.buf))
        # If we can exactly yield the bytes from the buffer, do so.
        self.assertIs(start, self.buf.consume_bytes(3))
        self.assertEqual(['cd', 'ef'], self.buf._content)
        self.assertEqual(4, len(self.buf))

    def test_consume_bytes_no_bytes(self):
        self.assertIs(None, self.buf.consume_bytes(1))

    def test_consume_bytes_not_enough_bytes(self):
        self.buf.add_bytes('ab')
        self.buf.add_bytes('cd')
        self.buf.add_bytes('ef')
        self.assertEqual(['ab', 'cd', 'ef'], self.buf._content)
        self.assertIs(None, self.buf.consume_bytes(7))
        self.assertEqual(['ab', 'cd', 'ef'], self.buf._content)

    def test_consume_partial_buffer(self):
        self.buf.add_bytes('ab')
        self.buf.add_bytes('cd')
        self.buf.add_bytes('ef')
        self.assertEqual(['ab', 'cd', 'ef'], self.buf._content)
        self.assertEqual(6, len(self.buf))
        self.assertIs('a', self.buf.consume_bytes(1))
        self.assertEqual(['b', 'cd', 'ef'], self.buf._content)
        self.assertEqual(5, len(self.buf))
        self.assertEqual('bc', self.buf.consume_bytes(2))
        self.assertEqual(['def'], self.buf._content)


class TestBufferedWriter(tests.TestCase):

    def setUp(self):
        super(TestBufferedWriter, self).setUp()
        self.sio = cStringIO.StringIO()
        self.writer = buffers.BufferedWriter(self.sio.write, 20)

    def test_no_write_less_than_max(self):
        self.writer.write('short content\n')
        self.writer.write('more\n')
        self.assertEqual('', self.sio.getvalue())
        self.assertEqual(['short content\n', 'more\n'],
                         self.writer._buf._content)

    def test_flush_long_after_enough_bytes(self):
        self.writer.write('abcd\nefgh\n')
        self.assertEqual('', self.sio.getvalue())
        self.writer.write('ijkl\nmnop\n')
        self.assertEqual('', self.sio.getvalue())
        self.writer.write('q')
        self.assertEqual('abcd\nefgh\nijkl\nmnop\nq', self.sio.getvalue())
        self.assertEqual([], self.writer._buf._content)

    def test_flush(self):
        self.writer.write('short content\n')
        self.writer.write('more\n')
        self.assertEqual('', self.sio.getvalue())
        self.writer.flush()
        self.assertEqual('short content\nmore\n', self.sio.getvalue())
        self.assertEqual([], self.writer._buf._content)


