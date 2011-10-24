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
    remote_requests,
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
        encoder.encode_dict('d', {'key': 'value'})
        self.assertEqual('d\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_dict_custom_type(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_dict('X', {'key': 'value'})
        self.assertEqual('X\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_end(self):
        sio, encoder = self.makeEncoder()
        encoder.encode_end()
        self.assertEqual('e\x00\x00\x00\x00', sio.getvalue())


class LoggingMessageHandler(object):
    """Just records what bits were observed by the protocol_decoder."""

    def __init__(self):
        self.actions = []

    def received_request_header(self, header):
        self.actions.append(('header', header))

    def received_request_args(self, args):
        self.actions.append(('args', args))

    def received_end(self):
        self.actions.append(('end',))


class TestProtocolDecoder(tests.TestCase):

    def makeDecoder(self):
        self.handler = LoggingMessageHandler()
        self.decoder = remote_sync_server.ProtocolDecoder(self.handler)
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
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1)
        self.assertEqual(decoder._state_expecting_structure,
                         decoder._state)
        self.assertEqual(0, len(decoder._buf))
        self.assertFalse(decoder.request_finished)

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
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1)
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
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1
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

    def test_process_bytes_after_request(self):
        decoder = self.makeDecoder()
        self.assertEqual(decoder._state_expecting_protocol_header,
                         decoder._state)
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1)
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
        decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1)
        self.assertEqual(remote_sync_server.PROTOCOL_HEADER_V1, decoder.unused_bytes())


class HelloRequest(object):

    def handle_args(self, **kwargs):
        self._args = kwargs

    def handle_end(self):
        self._finished = True


class TestStructureToRequest(tests.TestCase):

    def test_unknown_request(self):
        requests = {}
        handler = remote_sync_server.StructureToRequest(requests, None)
        e = self.assertRaises(errors.UnknownRequest,
            handler.received_request_header,
                {'client_version': '1', 'request': 'request-name'})
        self.assertIn('request-name', str(e))

    def test_initialize_request(self):
        requests = {"hello": HelloRequest}
        handler = remote_sync_server.StructureToRequest(requests, None)
        handler.received_request_header({'client_version': '1',
                                         'request': 'hello'})
        self.assertIsInstance(handler._request, HelloRequest)

    def test_call_args(self):
        requests = {"hello": HelloRequest}
        handler = remote_sync_server.StructureToRequest(requests, None)
        handler.received_request_header({'client_version': '1',
                                         'request': 'hello'})
        handler.received_request_args({'arg': 1})
        self.assertEqual(handler._request._args, {'arg': 1})

    def test_call_end(self):
        requests = {"hello": HelloRequest}
        handler = remote_sync_server.StructureToRequest(requests, None)
        handler.received_request_header({'client_version': '1',
                                         'request': 'hello'})
        handler.received_end()
        self.assertTrue(handler._request._finished)


class TestProtocolDecodingIntoRequest(tests.TestCase):

    def makeDecoder(self):
        requests = {'hello': HelloRequest}
        self.handler = remote_sync_server.StructureToRequest(requests, None)
        self.decoder = remote_sync_server.ProtocolDecoder(self.handler)
        return self.decoder

    def test_decode_full_request(self):
        self.makeDecoder()
        client_header = '{"client_version": "0.1.1.dev.0", "request": "hello"}'
        self.decoder.accept_bytes(remote_sync_server.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x35'
            + client_header
            + 'a\x00\x00\x00\x0E{"arg": "foo"}'
            + 'e\x00\x00\x00\x00')


class TestProtocolEncodeDecode(tests.TestCase):

    def test_simple_request(self):
        self.actions = []
        class TestFunc(remote_requests.RPCRequest):
            def __init__(fobj):
                # self here is the test case
                self.actions.append('initialized')
            def handle_args(fobj, **kwargs):
                self.actions.append(('args', kwargs))
            def handle_end(fobj):
                self.actions.append('end')
        requests = {'test': TestFunc}
        handler = remote_sync_server.StructureToRequest(requests, None)
        decoder = remote_sync_server.ProtocolDecoder(handler)
        encoder = remote_sync_server.ProtocolEncoderV1(decoder.accept_bytes)
        encoder.encode_request('test', arg1='a', arg2=2, value='bytes')
        self.assertEqual([
            'initialized',
            ('args', {'arg1': 'a', 'arg2': 2, 'value': 'bytes'}),
            'end',
            ], self.actions)


class TestResponder(tests.TestCase):

    def test__buffered_write(self):
        responder = remote_sync_server.Responder(None)
        responder._buffered_write('content\nbytes\n')
        responder._buffered_write('more content\n')
        self.assertEqual(['content\nbytes\n', 'more content\n'],
                         responder._out_buffer._content)

    def test_send_response(self):
        responder = remote_sync_server.Responder(None)
        responder.send_response(
            remote_requests.RPCSuccessfulResponse('request', value='success'))
        self.assertEqual(
            'u1db-1\n'
            'h%s{"server_version": "%s", "request": "request", "status": "success"}'
            % (struct.pack('!L', 65 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x14{"value": "success"}'
            + 'e\x00\x00\x00\x00',
            responder._out_buffer.peek_all_bytes())


class TestClient(tests.TestCase):

    def test__encode_request(self):
        sio = cStringIO.StringIO()
        encoder = remote_sync_server.ProtocolEncoderV1(sio.write)
        client = remote_sync_server.Client(None)
        client._encode_request(encoder, 'name', dict(a=1))
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "name"}'
            % (struct.pack('!L', 41 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x08{"a": 1}'
            + 'e\x00\x00\x00\x00',
            sio.getvalue())
