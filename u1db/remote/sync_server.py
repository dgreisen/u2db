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

import SocketServer
import threading

from u1db import (
    __version__ as _u1db_version,
    compat,
    buffers,
    errors,
    )
from u1db.remote import (
    protocol,
    requests,
    )


READ_CHUNK_SIZE = 64*1024
BUFFER_SIZE = 1024*1024


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


class TCPSyncRequestHandler(SocketServer.BaseRequestHandler):

    def setup(self):
        SocketServer.BaseRequestHandler.setup(self)
        self.finished = False

    def handle(self):
        extra_bytes = ''
        while not self.finished:
            extra_bytes = self._handle_one_request(extra_bytes)

    def _handle_one_request(self, extra_bytes):
        responder = Responder(self.request)
        handler = StructureToRequest(requests.RPCRequest.requests,
                                     responder)
        decoder = protocol.ProtocolDecoder(handler)
        if extra_bytes:
            decoder.accept_bytes(extra_bytes)
        while not decoder.request_finished:
            content = self.request.recv(READ_CHUNK_SIZE)
            if content == '':
                # We have been disconnected
                self.finished = True
                break
            decoder.accept_bytes(content)
        return decoder.unused_bytes()


class RemoteSyncServer(object):
    """Listen for requests to synchronize."""

    def __init__(self, db):
        self._db = db


class StructureToRequest(object):
    """Handle the parts of messages as they come in.

    Assign meaning to the structures received from the decoder.
    """

    def __init__(self, reqs, responder):
        self._request = None
        self._requests = reqs
        self._responder = responder
        self._client_version = None
        self._sent_response = False

    def received_header(self, headers):
        self._client_version = headers['client_version']
        self._lookup_request(headers['request'])
        self._check_send_response()

    def _lookup_request(self, request_name):
        factory = self._requests.get(request_name)
        if factory is None:
            raise errors.UnknownRequest(request_name)
        self._request = factory()

    def received_args(self, kwargs):
        self._request.handle_args(**kwargs)
        self._check_send_response()

    def received_end(self):
        self._request.handle_end()
        self._check_send_response()
        if self._request.response is None:
            raise errors.BadProtocolStream("Client sent end-of-message,"
                " but the Request did not generate a response."
                " for Request: %s" % (self._request,))

    def _check_send_response(self):
        if self._request.response is None or self._sent_response:
            return
        self._sent_response = True
        self._responder.send_response(self._request.response)


class Responder(object):
    """Encoder responses from the server back to the client."""

    def __init__(self, conn):
        """Turn an RPCResponse into bytes-on-the-wire."""
        self._conn = conn
        self._out_buffer = buffers.BufferedWriter(self._write_to_client,
            BUFFER_SIZE)
        self._encoder = protocol.ProtocolEncoderV1(self._out_buffer.write)

    def _write_to_client(self, content):
        self._conn.sendall(content)

    def send_response(self, response):
        """Send a RPCResponse back to the caller."""
        self._out_buffer.write(protocol.PROTOCOL_HEADER_V1)
        response_header = compat.OrderedDict([
            ('server_version', _u1db_version),
            ('request', response.request_name),
            ('status', response.status),
            ])
        self._encoder.encode_dict('h', response_header)
        if response.response_kwargs:
            self._encoder.encode_dict('a', response.response_kwargs)
        self._encoder.encode_end()
        self._out_buffer.flush()


