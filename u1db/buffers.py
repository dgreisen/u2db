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

"""smart-ish read/write buffers that play well with how python deals with memory."""


class Buffer(object):
    """Manager a buffer of bytes as they come in."""

    def __init__(self):
        self._content = []
        self._len = 0

    def __len__(self):
        return self._len

    def add_bytes(self, content):
        """Add more content to the internal buffer."""
        self._content.append(content)
        self._len += len(content)

    def peek_all_bytes(self):
        if not self._content:
            return ''
        if len(self._content) == 1:
            return self._content[0]
        content = ''.join(self._content)
        self._content = [content]
        return content

    def peek_bytes(self, count):
        """Check in the buffer for more content."""
        if count > self._len:
            # Not enough bytes for the peek, do nothing
            return None
        if len(self._content[0]) >= count:
            # We have enough bytes in the first byte string, return it
            return self._content[0][:count]
        # Join the buffer, return the count we need, and save the big buffer
        content = ''.join(self._content)
        self._content = [content]
        return content[:count]

    def peek_line(self):
        """Peek in the buffer for a line.

        This will return None if no line is available.
        """
        if not self._content:
            return None
        content = self._content[0]
        pos = content.find('\n')
        if pos == -1:
            # No newline in the first buffer, combine it, and try again
            content = ''.join(self._content)
            self._content = [content]
            pos = content.find('\n')
        if pos == -1:
            # No newlines at all
            return None
        pos += 1 # Move pos by 1 so we include '\n'
        if pos == len(content):
            # The newline fits exactly in the first chunk, return it
            return content
        return content[:pos]

    def consume_bytes(self, count):
        """Remove bytes from the buffer."""
        content = self.peek_bytes(count)
        if content is None:
            return None
        if len(content) != count:
            raise AssertionError('How did we get %d bytes when we asked for %d'
                                 % (len(content), count))
        if count == len(self._content[0]):
            self._content.pop(0)
        else:
            self._content[0] = self._content[0][count:]
        self._len -= count
        return content


class BufferedWriter(object):
    """Buffer writing to some output.
    """

    def __init__(self, writer, max_buf):
        self._buf = Buffer()
        self._writer = writer
        self._max_buf = max_buf

    def write(self, content):
        self._buf.add_bytes(content)
        if len(self._buf) > self._max_buf:
            self.flush()

    def flush(self):
        """Write whatever is buffered out to the real writer."""
        content = self._buf.peek_all_bytes()
        self._buf.consume_bytes(len(content))
        self._writer(content)



