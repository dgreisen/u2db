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

import struct
import SocketServer
import threading

import simplejson

from u1db import (
    __version__ as _u1db_version,
    compat,
    )

MESSAGE_HEADER = 'u1db-1\n'

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


class TCPSyncRequestHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        pass


class RemoteSyncServer(object):
    """Listen for requests to synchronize."""

    def __init__(self, db):
        self._db = db


class ProtocolEncoderV1(object):
    """Encode/decode a message."""

    def __init__(self, writer):
        self._writer = writer

    def encode_dict(self, d, dict_type='d'):
        raw = simplejson.dumps(d)
        l = struct.pack('>L', len(raw))
        self._writer(dict_type + l + raw)

    def encode_end(self):
        self._writer('e')

    def encode_request(self, request_name, **request_kwargs):
        request = compat.OrderedDict([
            ('client_version', _u1db_version),
            ('request', request_name),
            ])
        self.encode_dict(request, dict_type='h')
        self.encode_dict(request_kwargs, dict_type='a')
        self.encode_end()
