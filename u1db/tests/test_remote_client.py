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

"""Tests for the Client side of the remote protocol."""

import struct

from u1db import (
    __version__ as _u1db_version,
    tests,
    )
from u1db.remote import (
    client,
    protocol,
    requests,
    sync_server,
    )
from u1db.tests import test_remote_sync_server


class TestStructureToResponse(tests.TestCase):

    def test_received_header(self):
        response_handler = client.StructureToResponse()
        response_handler.received_header(
            {'server_version': '1', 'request': 'hello', 'status': 'success'})
        self.assertEqual('hello', response_handler.request_name)
        self.assertEqual('1', response_handler.server_version)
        self.assertEqual('success', response_handler.status)
        self.assertEqual(None, response_handler.kwargs)
        self.assertFalse(response_handler.finished)

    def test_received_args(self):
        response_handler = client.StructureToResponse()
        response_handler.received_header(
            {'server_version': '1', 'request': 'hello', 'status': 'success'})
        response_handler.received_args({'arg': 2, 'arg2': 'value'})
        self.assertEqual({'arg': 2, 'arg2': 'value'}, response_handler.kwargs)
        self.assertFalse(response_handler.finished)

    def test_received_stream_entry(self):
        entries = []
        def take_entry(entry):
            entries.append(entry)
        response_handler = client.StructureToResponse(take_entry)
        response_handler.received_header(
            {'server_version': '1', 'request': 'hello', 'status': 'success'})
        response_handler.received_stream_entry({'entry': True})
        self.assertEqual([{'entry': True}], entries)
        self.assertFalse(response_handler.finished)

    def test_received_end(self):
        response_handler = client.StructureToResponse()
        response_handler.received_header(
            {'server_version': '1', 'request': 'hello', 'status': 'success'})
        response_handler.received_args({'arg': 2, 'arg2': 'value'})
        response_handler.received_end()
        self.assertTrue(response_handler.finished)


class WithStreamRequest(requests.RPCRequest):

    name = 'withstream'

    def __init__(self, state, responder):
        super(WithStreamRequest, self).__init__(state, responder)

    def handle_args(self, **kwargs):
        self.responder.start_response(**kwargs)

    def handle_stream_entry(self, entry):
        v = entry['to_server'] * 5
        self.responder.stream_entry({'to_client': v})

    def handle_end(self):
        self.responder.finish_response()


class TestClient(tests.TestCase):

    def test__encode_request(self):
        server_sock, client_sock = tests.socket_pair()
        cli = client.Client(client_sock)
        cli._encode_request('name', dict(a=1))
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "name"}'
            % (struct.pack('!L', 41 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x08{"a": 1}'
            + 'e\x00\x00\x00\x00',
            server_sock.recv(4096))

    def test__encode_request_no_args(self):
        server_sock, client_sock = tests.socket_pair()
        cli = client.Client(client_sock)
        cli._encode_request('name', {})
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "name"}'
            % (struct.pack('!L', 41 + len(_u1db_version)), _u1db_version)
            + 'e\x00\x00\x00\x00',
            server_sock.recv(4096))

    def test__encode_request_with_stream(self):
        server_sock, client_sock = tests.socket_pair()
        cli = client.Client(client_sock)
        entry_source = client.EntrySource([{'stream_entry': 1},
                                           {'stream_entry': 2}])
        cli._encode_request('name', dict(a=1), entry_source_cb=entry_source.cb)
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "name"}'
            % (struct.pack('!L', 41 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x08{"a": 1}'
            + 's\x00\x00\x00\x13{"stream_entry": 1}'
            + 's\x00\x00\x00\x13{"stream_entry": 2}'
            + 'e\x00\x00\x00\x00',
            server_sock.recv(4096))

    def test_client_to_server_and_back(self):
        server_sock, client_sock = tests.socket_pair()
        cli = client.Client(client_sock)
        cli._encode_request('arg', {'one': 1})
        reqs = {'arg': test_remote_sync_server.ArgRequest}
        responder = sync_server.Responder(server_sock)
        handler = sync_server.StructureToRequest(reqs, responder,
            tests.ServerStateForTests())
        decoder = protocol.ProtocolDecoder(handler)
        # This should be the message from the client to the server
        content = server_sock.recv(4096)
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "arg"}'
            % (struct.pack('!L', 40 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x0a{"one": 1}'
            + 'e\x00\x00\x00\x00',
            content)
        decoder.accept_bytes(content)
        # The response from the server
        content = client_sock.recv(4096)
        self.assertEqual(
            'u1db-1\n'
            'h%s{"server_version": "%s", "request": "arg", "status": "success"}'
            % (struct.pack('!L', 61 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x0a{"one": 1}'
            + 'e\x00\x00\x00\x00',
            content)
        response_handler = client.StructureToResponse()
        decoder = protocol.ProtocolDecoder(response_handler)
        decoder.accept_bytes(content)
        self.assertEqual({'one': 1}, response_handler.kwargs)
        self.assertEqual('arg', response_handler.request_name)
        self.assertEqual(_u1db_version, response_handler.server_version)
        self.assertEqual('success', response_handler.status)
        self.assertTrue(response_handler.finished)

    def test_client_to_server_and_back_with_streaming(self):
        server_sock, client_sock = tests.socket_pair()
        cli = client.Client(client_sock)
        class ToServerEntrySource(client.EntrySource):
            @staticmethod
            def prepare(entry):
                return {'to_server': entry}
        entry_source = ToServerEntrySource([10, 20])
        cli._encode_request('withstream', {'one': 1},
                            entry_source_cb=entry_source.cb)
        reqs = {'withstream': WithStreamRequest}
        responder = sync_server.Responder(server_sock)
        handler = sync_server.StructureToRequest(reqs, responder,
            tests.ServerStateForTests())
        decoder = protocol.ProtocolDecoder(handler)
        # This should be the message from the client to the server
        content = server_sock.recv(4096)
        self.assertEqual(
            'u1db-1\n'
            'h%s{"client_version": "%s", "request": "withstream"}'
            % (struct.pack('!L', 47 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x0a{"one": 1}'
            + 's\x00\x00\x00\x11{"to_server": 10}'
            + 's\x00\x00\x00\x11{"to_server": 20}'
            + 'e\x00\x00\x00\x00',
            content)
        decoder.accept_bytes(content)
        # The response from the server
        content = client_sock.recv(4096)
        self.assertEqual(
            'u1db-1\n'
            'h%s{"server_version": "%s", "request": "withstream", '
            '"status": "success"}'
            % (struct.pack('!L', 68 + len(_u1db_version)), _u1db_version)
            + 'a\x00\x00\x00\x0a{"one": 1}'
            + 's\x00\x00\x00\x11{"to_client": 50}'
            + 's\x00\x00\x00\x12{"to_client": 100}'
            + 'e\x00\x00\x00\x00',
            content)
        entries = []
        def take_entry(entry):
            entries.append(entry)
        response_handler = client.StructureToResponse(take_entry)
        decoder = protocol.ProtocolDecoder(response_handler)
        decoder.accept_bytes(content)
        self.assertEqual([{'to_client': 50}, {'to_client': 100}], entries)
        self.assertEqual({'one': 1}, response_handler.kwargs)
        self.assertEqual('withstream', response_handler.request_name)
        self.assertEqual(_u1db_version, response_handler.server_version)
        self.assertEqual('success', response_handler.status)
        self.assertTrue(response_handler.finished)