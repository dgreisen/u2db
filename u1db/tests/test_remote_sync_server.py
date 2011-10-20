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

from u1db import (
    errors,
    remote_sync_server,
    tests,
    )
from u1db.backends import inmemory


class TestRemoteSyncServer(tests.TestCase):

    def test_takes_database(self):
        db = inmemory.InMemoryDatabase('test')
        server = remote_sync_server.RemoteSyncServer(db)


class MyHelloHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        value = self.rfile.readline()
        if value == 'hello\n':
            self.wfile.write('hello to you, too\n')


class TwoMessageHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        value = self.rfile.readline()
        self.wfile.write('same to you\n')
        value = self.rfile.readline()
        self.wfile.write('goodbye\n')


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
        self.addCleanup(self.server.shutdown)

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
