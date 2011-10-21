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

"""Tests for the remote synchronization server"""

import cStringIO
import threading
import socket
import SocketServer
import struct

from u1db import (
    __version__ as _u1db_version,
    errors,
    remote_sync_server,
    tests,
    )
from u1db.backends import inmemory


class TestRemoteSyncServer(tests.TestCase):

    def test_takes_database(self):
        db = inmemory.InMemoryDatabase('test')
        server = remote_sync_server.RemoteSyncServer(db)


class MyHelloHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        value = self.request.recv(1024)
        if value == 'hello\n':
            self.request.sendall('hello to you, too\n')


class TwoMessageHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        value = self.request.recv(1024)
        self.request.sendall('same to you\n')
        value = self.request.recv(1024)
        if value == '':
            return
        self.request.sendall('goodbye\n')


class TestBuffer(tests.TestCase):

    def setUp(self):
        super(TestBuffer, self).setUp()
        self.buf = remote_sync_server.Buffer()

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


class TestTCPSyncServer(tests.TestCase):

    def startServer(self, request_handler):
        self.server = remote_sync_server.TCPSyncServer(
            ('127.0.0.1', 0), request_handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever,
                                              kwargs=dict(poll_interval=0.01))
        self.server_thread.start()
        # Because TCPSyncServer sets bind_and_activate=True in its constructor,
        # the socket is already created. So the client can bind, even if the
        # server thread isn't actively listening yet.
        self.addCleanup(self.server_thread.join)
        self.addCleanup(self.server.force_shutdown)

    def connectToServer(self):
        client_sock = socket.socket()
        client_sock.connect(self.server.server_address)
        return client_sock

    def test_start_and_stop_server_in_a_thread(self):
        self.startServer(remote_sync_server.TCPSyncRequestHandler)
        self.server.shutdown()
        self.server_thread.join()

    def test_say_hello(self):
        self.startServer(MyHelloHandler)
        client_sock = self.connectToServer()
        client_sock.sendall('hello\n')
        self.assertEqual('hello to you, too\n', client_sock.recv(1024))
        client_sock.close()

    def test_tracks_active_connections(self):
        self.startServer(TwoMessageHandler)
        client_sock = self.connectToServer()
        client_sock.sendall('hello\n')
        self.assertEqual('same to you\n', client_sock.recv(1024))
        self.assertEqual(1, len(self.server._request_threads))
        client_sock.sendall('goodbye\n')
        self.assertEqual('goodbye\n', client_sock.recv(1024))

    def test_wait_for_requests(self):
        # This can be called after shutdown() if you want to wait for all the
        # request threads to finish.
        self.startServer(TwoMessageHandler)
        client_sock = self.connectToServer()
        client_sock.sendall('hello\n')
        self.assertEqual('same to you\n', client_sock.recv(1024))
        started = threading.Event()
        returned = threading.Event()
        self.server.shutdown()
        def waited_and_returned():
            started.set()
            self.server.wait_for_requests()
            returned.set()
        waiting_thread = threading.Thread(target=waited_and_returned)
        waiting_thread.start()
        started.wait(10.0)
        if not started.isSet():
            self.fail('started never reached')
        returned.wait(0.01)
        self.assertFalse(returned.isSet())
        client_sock.sendall('goodbye\n')
        self.assertEqual('goodbye\n', client_sock.recv(1024))
        returned.wait(10.0)
        if not returned.isSet():
            self.fail('returned never reached')

    def test_force_close_requests(self):
        self.startServer(TwoMessageHandler)
        client_sock = self.connectToServer()
        client_sock.sendall('hello\n')
        self.assertEqual('same to you\n', client_sock.recv(1024))
        # Now, we forcibly close all active connections, rather than doing a
        # graceful shutdown. (shutdown(), then wait_for_requests())
        self.server.force_shutdown()
        # the request thread has been requested to close, however, it doesn't
        # seem to actually close on all platforms until the client socket is
        # closed, so we can't assert the socket is closed by reading the empty
        # string on it.
        # self.assertEqual('', client_sock.recv(1024))


class TestProtocolEncoderV1(tests.TestCase):

    def makeEncoder(self):
        sio = cStringIO.StringIO()
        encoder = remote_sync_server.ProtocolEncoderV1(sio.write)
        return sio, encoder

    def test_encode_dict(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_dict({'key': 'value'})
        self.assertEqual('d\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_dict_custom_type(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_dict({'key': 'value'}, dict_type='X')
        self.assertEqual('X\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_end(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_end()
        self.assertEqual('e\x00\x00\x00\x00', sio.getvalue())

    def test_encode_request(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_request('name', a=1)
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "name"}'
            % (struct.pack('!L', 41 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x08{"a": 1}'
            + 'e\x00\x00\x00\x00',
            sio.getvalue())


class LoggingMessageHandler(object):
    """Just records what bits were observed by the protocol_decoder."""

    def __init__(self):
        self.actions = []

    def received_structure(self, structure_type, value):
        self.actions.append(('structure', structure_type, value))


class TestProtocolDecoderV1(tests.TestCase):

    def makeDecoder(self):
        self.messages = LoggingMessageHandler()
        self.decoder = remote_sync_server.ProtocolDecoderV1(self.messages)
        return self.decoder

    def test_starting_state(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)

    def test_process_header(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1)
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        self.assertEqual(0, len(decoder._buf))

    def test_process_partial_header(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1[:3])
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1[3:])
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)

    def test_process_bad_header(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        e = self.assertRaises(errors.UnknownProtocolVersion,
                          decoder.accept_bytes, 'Not A Protocol\n')
        self.assertIn('Not A Protocol', str(e))
        # The bytes haven't been consumed, either
        self.assertEqual('Not A Protocol\n', decoder.unused_bytes())

    def test_process_proto_and_partial_structure(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1)
        self.assertEqual([], self.messages.actions)
        # Not enough bytes for a structure
        decoder.accept_bytes('s')
        self.assertEqual([], self.messages.actions)
        self.assertEqual('s', decoder.unused_bytes())
        decoder.accept_bytes('\x00\x00\x00\x00')
        self.assertEqual([('structure', 's', '')], self.messages.actions)
        self.assertEqual('', decoder.unused_bytes())

    def test_process_proto_and_request(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        client_header = '{"client_version": "0.1.1.dev.0", "request": "foo"}'
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x33'
            + client_header
            + 'e\x00\x00\x00\x00')
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        self.assertEqual([('structure', 'h', client_header),
                          ('structure', 'e', '')],
                         self.messages.actions)
        self.assertEqual('', decoder.unused_bytes())


class TestMessageHandler(tests.TestCase):

    def test_header(self):
        handler = remote_sync_server.MessageHandler()
        handler.received_structure('h',
            '{"client_version": "0.1.1.dev.0", "request": "foo"}')
        self.assertEqual(handler._cur_message.client_version, '0.1.1.dev.0')
        self.assertEqual(handler._cur_message.request, 'foo')

    def test_args(self):
        handler = remote_sync_server.MessageHandler()
        handler.received_structure('a', '{"a": 1, "b": "foo"}')
        self.assertEqual(handler._cur_message.args, {'a': 1, 'b': 'foo'})

    def test_end(self):
        handler = remote_sync_server.MessageHandler()
        handler.received_structure('e', '')
        self.assertEqual(handler._cur_message.complete, True)

    def test_unknown_structure(self):
        handler = remote_sync_server.MessageHandler()
        e = self.assertRaises(errors.BadProtocolStream,
            handler.received_structure, 'z', '')
        self.assertEqual("unknown structure type: 'z'", str(e))


class TestProtocolDecodingIntoMessage(tests.TestCase):

    def makeDecoder(self):
        self.handler = remote_sync_server.MessageHandler()
        self.decoder = remote_sync_server.ProtocolDecoderV1(self.handler)
        return self.decoder

    def test_decode_full_request(self):
        self.makeDecoder()
        client_header = '{"client_version": "0.1.1.dev.0", "request": "foo"}'
        self.decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x33'
            + client_header
            + 'a\x00\x00\x00\x0E{"arg": "foo"}'
            + 'e\x00\x00\x00\x00')
        message = self.handler._cur_message
        self.assertEqual('0.1.1.dev.0', message.client_version)
        self.assertEqual('foo', message.request)
        self.assertEqual({'arg': 'foo'}, message.args)
        self.assertTrue(message.complete)


class TestProtocolEncodeDecode(tests.TestCase):

    def test_simple_request(self):
        handler = remote_sync_server.MessageHandler()
        decoder = remote_sync_server.ProtocolDecoderV1(handler)
        encoder = remote_sync_server.ProtocolEncoderV1(decoder.accept_bytes)
        encoder.encode_request('myrequest', arg1='a', arg2=2, value='bytes')
        message = handler._cur_message
        self.assertEqual(_u1db_version, message.client_version)
        self.assertEqual('myrequest', message.request)
        self.assertEqual({'arg1': 'a', 'arg2': 2, 'value':'bytes'},
                         message.args)
