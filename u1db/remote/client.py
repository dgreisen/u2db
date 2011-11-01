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

"""The Client side of the Remote interface"""

from u1db import (
    __version__ as _u1db_version,
    buffers,
    compat,
    SyncTarget,
    )
from u1db.remote import (
    protocol,
    sync_server,
    )

READ_CHUNK_SIZE = sync_server.READ_CHUNK_SIZE
BUFFER_SIZE = sync_server.BUFFER_SIZE


class StructureToResponse(object):
    """Take structured byte streams and turn them into a Response."""

    def __init__(self, return_entry_cb=None):
        self.request_name = None
        # self._responder = responder
        self.server_version = None
        self.status = None
        self.return_entry_cb = return_entry_cb
        self.kwargs = None
        self.finished = False

    def received_header(self, headers):
        self.server_version = headers['server_version']
        self.request_name = headers['request']
        self.status = headers['status']

    def received_stream_entry(self, entry):
        self.return_entry_cb(entry)

    def received_args(self, kwargs):
        self.kwargs = kwargs

    def received_end(self):
        self.finished = True


class EntrySource(object):

    def __init__(self, entries):
        self.i = 0
        self.entries = entries

    @staticmethod
    def prepare(self, entry):
        return entry

    def cb(self):
        if self.i >= len(self.entries):
            return None
        entry = self.prepare(self.entries[self.i])
        self.i += 1
        return entry


class Client(object):
    """Implement the client-side managing the call state."""

    def __init__(self, conn):
        self._conn = conn
        self._cur_request = None

    def _write_to_server(self, content):
        self._conn.sendall(content)

    def _read_from_server(self):
        return self._conn.recv(READ_CHUNK_SIZE)

    def _encode_request(self, request_name, kwargs, entry_source_cb=None):
        buf = buffers.BufferedWriter(self._write_to_server, BUFFER_SIZE)
        buf.write(protocol.PROTOCOL_HEADER_V1)
        encoder = protocol.ProtocolEncoderV1(buf.write)
        request_header = compat.OrderedDict([
            ('client_version', _u1db_version),
            ('request', request_name),
            ])
        encoder.encode_dict('h', request_header)
        if kwargs:
            encoder.encode_dict('a', kwargs)
        if entry_source_cb:
            while True:
                entry = entry_source_cb()
                if entry is None:
                    break
                encoder.encode_dict('s', entry)
        encoder.encode_end()
        buf.flush()

    def _read_more_content(self, decoder):
        content = self._read_from_server()
        if content == '':
            fail
            # Connection prematurely closed while waiting for a response.
        decoder.accept_bytes(content)

    # Not used yet.
    # def _wait_for_response_header(self):
    #     """Keep reading from the connection until we see the response args."""
    #     while (not response_handler.finished
    #            and response_handler.status is None):
    #         self._read_more_content()

    # def _wait_for_response_args(self):
    #     while (not response_handler.finished
    #            and response_handler.kwargs is None):
    #         self._read_more_content()

    def _wait_for_response_end(self, response_handler, decoder):
        while not response_handler.finished:
            self._read_more_content(decoder)

    def call_returning_args(self, rpc_name, **kwargs):
        """Place a call to the remote server.

        This call assumes the server is just going to return simple arguments
        back to the client.
        """
        self._encode_request(rpc_name, kwargs)
        response_handler = StructureToResponse()
        decoder = protocol.ProtocolDecoder(response_handler)
        self._wait_for_response_end(response_handler, decoder)
        return response_handler.kwargs

    def call_with_streaming(self, rpc_name, entry_source_cb, return_entry_cb,
                            **kwargs):
        """Place a call to the remote server.

        Send and expect streams of documents.
        """
        self._encode_request(rpc_name, kwargs, entry_source_cb)
        response_handler = StructureToResponse(return_entry_cb)
        decoder = protocol.ProtocolDecoder(response_handler)
        self._wait_for_response_end(response_handler, decoder)
        return response_handler.kwargs
