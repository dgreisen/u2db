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

"""An encoding and decoding protocol."""

import simplejson
import struct

from u1db import (
    __version__ as _u1db_version,
    buffers,
    compat,
    errors,
    )

PROTOCOL_HEADER_V1 = 'u1db-1\n'


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


class _ProtocolDecoderV1(object):

    def __init__(self, buf, structure_handler):
        self._buf = buf
        self._structure_handler = structure_handler

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
            self._structure_handler.received_header(
                simplejson.loads(content))
        elif struct_type == 'a':
            self._structure_handler.received_args(
                simplejson.loads(content))
        elif struct_type == 'e':
            # assert content == ''
            self._structure_handler.received_end()
        return struct_type


class ProtocolDecoder(object):
    """Generic decoding of structured data."""

    def __init__(self, structure_handler):
        self._state = self._state_expecting_protocol_header
        self._structure_handler = structure_handler
        self._buf = buffers.Buffer()
        self._decoder = None
        self.request_finished = False

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
        self._decoder = _ProtocolDecoderV1(self._buf, self._structure_handler)
        self._state = self._state_expecting_structure
        return True

    def _state_expecting_structure(self):
        res = self._decoder.decode_one()
        if res is None:
            return False
        if res == 'e': # End of request
            self._state = self._state_finished
            self.request_finished = True
        return True

    def _state_finished(self):
        # We won't transition to another state from here, this is used to allow
        # accept_bytes to buffer any extra bytes to be used for the next
        # request.
        return False


