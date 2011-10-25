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

import threading
import socket
import SocketServer
import struct

from u1db import (
    __version__ as _u1db_version,
    errors,
    tests,
    )
from u1db.remote import (
    client,
    protocol,
    requests,
    sync_server,
    )
from u1db.backends import inmemory


class TestRemoteSyncServer(tests.TestCase):

    def test_takes_database(self):
        db = inmemory.InMemoryDatabase('test')
        server = sync_server.RemoteSyncServer(db)


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
        self.server = sync_server.TCPSyncServer(
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
        self.addCleanup(client_sock.close)
        return client_sock

    def test_start_and_stop_server_in_a_thread(self):
        self.startServer(sync_server.TCPSyncRequestHandler)
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

    def test_rpc_version_bytes(self):
        self.startServer(sync_server.TCPSyncRequestHandler)
        client_sock = self.connectToServer()
        client_sock.sendall(protocol.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x31{"client_version": "0.1.1", "request": "version"}'
            + 'e\x00\x00\x00\x00')
        self.assertEqual(protocol.PROTOCOL_HEADER_V1
            + 'h%s{"server_version": "%s", "request": "version", "status": "success"}'
              % (struct.pack('!L', 65 + len(_u1db_version)), _u1db_version)
            + 'a%s{"version": "%s"}'
              % (struct.pack('!L', 15 + len(_u1db_version)), _u1db_version)
            + 'e\x00\x00\x00\x00',
            client_sock.recv(4096))
        client_sock.close()

    def test_client_rpc_version(self):
        self.startServer(sync_server.TCPSyncRequestHandler)
        client_sock = self.connectToServer()
        cli = client.Client(client_sock)
        self.assertEqual({'version': _u1db_version},
                         cli.call_returning_args('version'))



class HelloRequest(requests.RPCRequest):

    name = 'hello'

    def handle_args(self, **kwargs):
        self._args = kwargs

    def handle_end(self):
        self._finished = True
        self.response = requests.RPCSuccessfulResponse(self.name)


class FastRequest(requests.RPCRequest):

    name = 'fast'

    def __init__(self):
        self.response = requests.RPCSuccessfulResponse(self.name,
            value=True)


class ArgRequest(requests.RPCRequest):

    name = 'arg'

    def handle_args(self, **kwargs):
        self.response = requests.RPCSuccessfulResponse(self.name,
            **kwargs)


class EndRequest(requests.RPCRequest):

    name = 'end'

    def handle_end(self):
        self.response = requests.RPCSuccessfulResponse(self.name,
            finished=True)


class ResponderForTests(object):

    def __init__(self):
        self.response = None

    def send_response(self, response):
        self.response = response


class TestStructureToRequest(tests.TestCase):

    def makeStructToRequest(self):
        reqs = {"hello": HelloRequest, 'fast': FastRequest,
                    "arg": ArgRequest, 'end': EndRequest}
        responder = ResponderForTests()
        handler = sync_server.StructureToRequest(reqs, responder)
        return handler

    def test_unknown_request(self):
        handler = self.makeStructToRequest()
        e = self.assertRaises(errors.UnknownRequest,
            handler.received_header,
                {'client_version': '1', 'request': 'request-name'})
        self.assertIn('request-name', str(e))

    def test_initialize_request(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1',
                                 'request': 'hello'})
        self.assertIsInstance(handler._request, HelloRequest)

    def test_call_args(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1',
                                 'request': 'hello'})
        handler.received_args({'arg': 1})
        self.assertEqual(handler._request._args, {'arg': 1})

    def test_call_end(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1',
                                 'request': 'hello'})
        handler.received_end()
        self.assertTrue(handler._request._finished)

    def test_send_response_after_header(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1', 'request': 'fast'})
        self.assertIsNot(None, handler._responder.response)
        handler.received_end()

    def test_send_response_after_args(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1', 'request': 'arg'})
        self.assertIs(None, handler._responder.response)
        handler.received_args({'arg': 'value', 'foo': 1})
        self.assertIsNot(None, handler._responder.response)
        handler.received_end()

    def test_send_response_after_end(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1', 'request': 'end'})
        self.assertIs(None, handler._responder.response)
        handler.received_end()
        self.assertIsNot(None, handler._responder.response)

    def test_end_no_response(self):
        handler = self.makeStructToRequest()
        handler.received_header({'client_version': '1', 'request': 'arg'})
        self.assertIs(None, handler._responder.response)
        self.assertRaises(errors.BadProtocolStream,
                          handler.received_end)


class TestProtocolDecodingIntoRequest(tests.TestCase):

    def makeDecoder(self):
        responder = ResponderForTests()
        reqs = {'hello': HelloRequest}
        self.handler = sync_server.StructureToRequest(
            reqs, responder)
        self.decoder = protocol.ProtocolDecoder(self.handler)
        return self.decoder

    def test_decode_full_request(self):
        self.makeDecoder()
        client_header = '{"client_version": "0.1.1.dev.0", "request": "hello"}'
        self.decoder.accept_bytes(protocol.PROTOCOL_HEADER_V1
            + 'h\x00\x00\x00\x35'
            + client_header
            + 'a\x00\x00\x00\x0E{"arg": "foo"}'
            + 'e\x00\x00\x00\x00')


class TestProtocolEncodeDecode(tests.TestCase):

    def test_simple_request(self):
        self.actions = []
        class TestFunc(requests.RPCRequest):
            name = 'test'
            def __init__(fobj):
                fobj.response = None
                # self here is the test case
                self.actions.append('initialized')
            def handle_args(fobj, **kwargs):
                self.actions.append(('args', kwargs))
            def handle_end(fobj):
                self.actions.append('end')
                fobj.response = requests.RPCSuccessfulResponse(fobj.name)
        reqs = {'test': TestFunc}
        responder = ResponderForTests()
        handler = sync_server.StructureToRequest(reqs, responder)
        decoder = protocol.ProtocolDecoder(handler)
        encoder = protocol.ProtocolEncoderV1(decoder.accept_bytes)
        encoder.encode_request('test', arg1='a', arg2=2, value='bytes')
        self.assertEqual([
            'initialized',
            ('args', {'arg1': 'a', 'arg2': 2, 'value': 'bytes'}),
            'end',
            ], self.actions)


class TestResponder(tests.TestCase):

    def test_send_response(self):
        server_sock, client_sock = tests.socket_pair()
        responder = sync_server.Responder(server_sock)
        responder.send_response(
            requests.RPCSuccessfulResponse('request', value='success'))
        self.assertEqual(
            'u1db-1\n'
            'h%s{"server_version": "%s", "request": "request", "status": "success"}'
            % (struct.pack('!L', 65 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x14{"value": "success"}'
            + 'e\x00\x00\x00\x00',
            client_sock.recv(4096))


