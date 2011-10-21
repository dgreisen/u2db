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

    def getEncoder(self):
        sio = cStringIO.StringIO()
        encoder = remote_sync_server.ProtocolEncoderV1(sio.write)
        return sio, encoder

    def test_encode_dict(self):
        sio, encoder = self.getEncoder()
        encoder.encode_dict({'key': 'value'})
        self.assertEqual('d\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_dict_custom_type(self):
        sio, encoder = self.getEncoder()
        encoder.encode_dict({'key': 'value'}, dict_type='X')
        self.assertEqual('X\x00\x00\x00\x10{"key": "value"}', sio.getvalue())

    def test_encode_end(self):
        sio, encoder = self.getEncoder()
        encoder.encode_end()
        self.assertEqual('e', sio.getvalue())

    def test_encode_request(self):
        sio, encoder = self.getEncoder()
        encoder.encode_request('name', a=1)
        self.assertEqual(
            'h%s{"client_version": "%s", "request": "name"}'
            % (struct.pack('!L', 41 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x08{"a": 1}'
            + 'e',
            sio.getvalue())
