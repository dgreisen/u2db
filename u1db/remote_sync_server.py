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

"""A Server that listens for synchronization requests"""

import struct
import SocketServer
import threading

import simplejson

from u1db import (
    __version__ as _u1db_version,
    compat,
    errors,
    )


PROTOCOL_HEADER_V1 = 'u1db-1\n'
READ_CHUNK_SIZE = 64*1024

class TCPSyncServer(SocketServer.TCPServer):

    allow_reuse_address = False # Should be set to True for testing
    daemon_threads = False

    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.TCPServer.__init__(self, server_address,
                                        RequestHandlerClass)
        self._request_threads = {}
        self._request_threads_lock = threading.Lock()

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.

        In addition, exception handling is done here.

        """
        try:
            self.finish_request(request, client_address)
            self.close_request(request)
        except:
            self.handle_error(request, client_address)
            self.close_request(request)

    def _add_request(self, request, t):
        with self._request_threads_lock:
            self._request_threads[request] = t

    def _remove_request(self, request):
        with self._request_threads_lock:
            if request in self._request_threads:
                t = self._request_threads.pop(request)
                return t
        return None

    def _get_a_request_thread(self):
        with self._request_threads_lock:
            if self._request_threads:
                return self._request_threads.itervalues().next()
        return None

    def process_request(self, request, client_address):
        """Start a new thread to process the request."""
        t = threading.Thread(target = self.process_request_thread,
                             args = (request, client_address))
        self._add_request(request, t)
        if self.daemon_threads:
            t.setDaemon (1)
        t.start()

    def close_request(self, request):
        SocketServer.TCPServer.close_request(self, request)
        return self._remove_request(request)

    def wait_for_requests(self):
        """Wait until all request threads have exited cleanly."""
        t = self._get_a_request_thread()
        while t is not None:
            t.join()
            t = self._get_a_request_thread()

    def force_shutdown(self):
        self.shutdown()
        waiting_threads = []
        with self._request_threads_lock:
            all_requests = self._request_threads.keys()
        for request in all_requests:
            t = self.close_request(request)
            if t is not None:
                waiting_threads.append(t)
        # Note: We don't wait on the threads. I've discovered that
        #       request.close() in one thread may not actually cause
        #       request.recv() to return in another thread. The only thing that
        #       seems to always work is client_sock.close()


class TCPSyncRequestHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        handler = MessageHandler()
        decoder = _ProtocolDecoderV1(handler)
        content = self.request.recv(READ_CHUNK_SIZE)
        while content:
            decoder.accept_bytes()
            content = self.request.recv(READ_CHUNK_SIZE)


class RemoteSyncServer(object):
    """Listen for requests to synchronize."""

    def __init__(self, db):
        self._db = db


class ProtocolEncoderV1(object):
    """Encode/decode a message."""

    def __init__(self, writer):
        self._writer = writer

    def encode_dict(self, dict_type, d):
        raw = simplejson.dumps(d)
        l = struct.pack('>L', len(raw))
        self._writer(dict_type + l + raw)

    def encode_end(self):
        self._writer('e\x00\x00\x00\x00')

    def encode_request(self, request_name, **request_kwargs):
        self._writer(PROTOCOL_HEADER_V1)
        request_header = compat.OrderedDict([
            ('client_version', _u1db_version),
            ('request', request_name),
            ])
        self.encode_dict('h', request_header)
        self.encode_dict('a', request_kwargs)
        self.encode_end()


class Message(object):
    """A single message, built up by MessageHandler"""

    def __init__(self):
        self.client_version = None
        self.request = None
        self.args = None
        self.complete = False

    def set_header(self, msg):
        self.client_version = msg['client_version']
        self.request = msg['request']

    def set_arguments(self, msg):
        self.args = msg

    def set_finished(self):
        self._validate()
        self.complete = True

    def _validate(self):
        pass


class RequestHandler(object):
    """Handle the parts of messages as they come in.

    Assign meaning to the structures received from the decoder.
    """

    def __init__(self, commands):
        self._request = None
        self._commands = commands
        self._client_version = None

    def received_request_header(self, headers):
        self._client_version = headers['client_version']
        self._lookup_request(headers['request'])

    def _lookup_request(self, request_name):
        factory = self._commands.get(request_name)
        if factory is None:
            raise errors.UnknownRequest(request_name)
        self._request = factory()

    def received_request_args(self, kwargs):
        self._request.handle_args(**kwargs)

    def received_end(self):
        self._request.handle_end()


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


class _ProtocolDecoderV1(object):

    def __init__(self, buf, message_handler):
        self._buf = buf
        self._message_handler = message_handler

    def _extract_tlv(self):
        """Decode a Type Length Value structure.

        The Type is one byte (we assign no meaning at this level),
        Length is a 4-byte big-endian integer.
        Value (at this level) is octet-stream of the given length.
        """
        type_len = self._buf.peek_bytes(5)
        if type_len is None:
            return None
        # TODO: We should probably validate struct_len isn't something crazy
        #       like 4GB, but for now, just accept whatever. Arbitrary message
        #       length caps would be... arbitrary.
        struct_type, struct_len = struct.unpack('>cL', type_len)
        # Consume bytes only moves the pointer if it is successful
        content = self._buf.consume_bytes(5 + struct_len)
        if content is None:
            return None
        return struct_type, content[5:]

    def decode_one(self):
        res = self._extract_tlv()
        if res is None:
            return None
        struct_type, content = res
        if struct_type == 'h':
            self._message_handler.received_request_header(
                simplejson.loads(content))
        elif struct_type == 'a':
            self._message_handler.received_request_args(
                simplejson.loads(content))
        elif struct_type == 'e':
            # assert content == ''
            self._message_handler.received_end()
        return struct_type


class ProtocolDecoder(object):
    """Generic decoding of structured data."""

    def __init__(self, message_handler):
        self._state = self._state_expecting_protocol_header
        self._message_handler = message_handler
        self._buf = Buffer()
        self._decoder = None

    def accept_bytes(self, content):
        """Some bytes have been read, process them."""
        self._buf.add_bytes(content)
        while self._state():
            pass

    def unused_bytes(self):
        return self._buf.peek_all_bytes()

    def _state_expecting_protocol_header(self):
        proto_header_bytes = self._buf.peek_line()
        if proto_header_bytes is None:
            # Not enough bytes for the v1 header yet
            return False
        if proto_header_bytes != PROTOCOL_HEADER_V1:
            raise errors.UnknownProtocolVersion(
                'expected protocol header: %r got: %r'
                % (PROTOCOL_HEADER_V1, proto_header_bytes))
        self._buf.consume_bytes(len(proto_header_bytes))
        self._decoder = _ProtocolDecoderV1(self._buf, self._message_handler)
        self._state = self._state_expecting_structure
        return True

    def _state_expecting_structure(self):
        res = self._decoder.decode_one()
        if res is None:
            return False
        if res == 'e': # End of request
            self._state = self._state_finished
        return True

    def _state_finished(self):
        # We won't transition to another state from here, this is used to allow
        # accept_bytes to buffer any extra bytes to be used for the next
        # request.
        return False
