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

"""Tests for encoding/decoding protocols"""

import cStringIO

from u1db import (
    errors,
    tests,
    )
from u1db.remote import protocol



class TestProtocolEncoderV1(tests.TestCase):

    def makeEncoder(self):
        sio = cStringIO.StringIO()
        encoder = protocol.ProtocolEncoderV1(sio.write)
        return sio, encoder

    def test_encode_dict(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_dict('d', {'key': 'value'})
        self.assertEqual('d\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_end(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_end()
        self.assertEqual('e\x00\x00\x00\x00', sio.getvalue())


class StructureToLogging(object):
    """Just records what bits were observed by the protocol_decoder."""

    def __init__(self):
        self.actions = []

    def received_header(self, header):
        self.actions.append(('header', header))

    def received_args(self, args):
        self.actions.append(('args', args))

    def received_stream_entry(self, stream_entry):
        self.actions.append(('stream', stream_entry))

    def received_end(self):
        self.actions.append(('end',))


class TestProtocolDecoder(tests.TestCase):

    def makeDecoder(self):
        self.handler = StructureToLogging()
        self.decoder = protocol.ProtocolDecoder(self.handler)
        return self.decoder

    def test_starting_state(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        self.assertFalse(decoder.request_finished)

    def test_process_header(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1)
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        self.assertEqual(0, len(decoder._buf))
        self.assertFalse(decoder.request_finished)

    def test_process_partial_header(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1[:3])
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1[3:])
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        self.assertFalse(decoder.request_finished)

    def test_process_bad_header(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        e = self.assertRaises(errors.UnknownProtocolVersion,
                          decoder.accept_bytes, 'Not A Protocol\n')
        self.assertIn('Not A Protocol', str(e))
        # The bytes haven't been consumed, either
        self.assertEqual('Not A Protocol\n', decoder.unused_bytes())
        self.assertFalse(decoder.request_finished)

    def test_process_proto_and_partial_structure(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1)
        self.assertFalse(decoder.request_finished)
        self.assertEqual([], self.handler.actions)
        # Not enough bytes for a structure
        decoder.accept_bytes('e')
        self.assertEqual([], self.handler.actions)
        self.assertEqual('e', decoder.unused_bytes())
        decoder.accept_bytes('\x00\x00\x00\x00')
        self.assertEqual([('end',)], self.handler.actions)
        self.assertEqual('', decoder.unused_bytes())
        self.assertTrue(decoder.request_finished)

    def test_process_proto_and_request(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        client_header = '{"client_version": "0.1.1.dev.0", "request": "foo"}'
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x33' + client_header
            + 'a\x00\x00\x00\x18{"arg": 1, "val": "bar"}'
            + 'e\x00\x00\x00\x00')
        self.assertEqual(decoder._state_finished, decoder._state)
        self.assertEqual([
            ('header', {'client_version': '0.1.1.dev.0', 'request': 'foo'}),
            ('args', {'arg': 1, 'val': 'bar'}),
            ('end',),
            ], self.handler.actions)
        self.assertEqual('', decoder.unused_bytes())
        self.assertTrue(decoder.request_finished)

    def test_process_proto_and_request_and_stream(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        client_header = '{"client_version": "0.1.1.dev.0", "request": "foo"}'
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x33' + client_header
            + 'a\x00\x00\x00\x18{"arg": 1, "val": "bar"}'
            + 'x\x00\x00\x00\x13{"stream_entry": 1}'
            + 'e\x00\x00\x00\x00')
        self.assertEqual(decoder._state_finished, decoder._state)
        self.assertEqual([
            ('header', {'client_version': '0.1.1.dev.0', 'request': 'foo'}),
            ('args', {'arg': 1, 'val': 'bar'}),
            ('stream', {'stream_entry': 1}),
            ('end',),
            ], self.handler.actions)
        self.assertEqual('', decoder.unused_bytes())
        self.assertTrue(decoder.request_finished)

    def test_process_bytes_after_request(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1)
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        decoder.accept_bytes(
            'h\x00\x00\x00\x33{"client_version": "0.1.1.dev.0", "request": "foo"}')
        self.assertFalse(decoder.request_finished)
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        self.assertEqual([
            ('header', {'client_version': '0.1.1.dev.0', 'request': 'foo'}),
            ], self.handler.actions)
        self.assertEqual('', decoder.unused_bytes())
        decoder.accept_bytes('e\x00\x00\x00\x00')
        self.assertEqual(decoder._state_finished, decoder._state)
        self.assertTrue(decoder.request_finished)
        self.assertEqual('', decoder.unused_bytes())
        decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1)
        self.assertEqual(protocol.PROTOCOL_HEADER_V1, decoder.unused_bytes())


