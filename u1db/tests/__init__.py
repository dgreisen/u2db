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

"""Test infrastructure for U1DB"""

import shutil
import socket
import tempfile
import threading

import testscenarios
import testtools

from u1db.backends import (
    inmemory,
    sqlite_backend,
    )
from u1db.remote import (
    requests,
    sync_server,
    )


class TestCase(testtools.TestCase):

    def createTempDir(self, prefix='u1db-tmp-'):
        """Create a temporary directory to do some work in.

        This directory will be scheduled for cleanup when the test ends.
        """
        tempdir = tempfile.mkdtemp(prefix=prefix)
        self.addCleanup(shutil.rmtree, tempdir)
        return tempdir


simple_doc = '{"key": "value"}'
nested_doc = '{"key": "value", "sub": {"doc": "underneath"}}'


def create_memory_database(replica_uid):
    return inmemory.InMemoryDatabase(replica_uid)


def create_sqlite_expanded(replica_uid):
    db = sqlite_backend.SQLiteExpandedDatabase(':memory:')
    db._set_replica_uid(replica_uid)
    return db


def create_sqlite_partial_expanded(replica_uid):
    db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
    db._set_replica_uid(replica_uid)
    return db


def create_sqlite_only_expanded(replica_uid):
    db = sqlite_backend.SQLiteOnlyExpandedDatabase(':memory:')
    db._set_replica_uid(replica_uid)
    return db


class DatabaseBaseTests(TestCase):

    create_database = None
    scenarios = [
        ('mem', {'create_database': create_memory_database}),
        ('sql_expand', {'create_database': create_sqlite_expanded}),
        ('sql_partexpand', {'create_database': create_sqlite_partial_expanded}),
        ('sql_onlyexpand', {'create_database': create_sqlite_only_expanded}),
        ]

    def setUp(self):
        super(DatabaseBaseTests, self).setUp()
        self.db = self.create_database('test')

    def tearDown(self):
        # TODO: Add close_database parameterization
        # self.close_database(self.db)
        super(DatabaseBaseTests, self).tearDown()


class ServerStateForTests(requests.ServerState):
    """Used in the test suite, so we don't have to touch disk, etc."""

    def __init__(self):
        super(ServerStateForTests, self).__init__()
        self._dbs = {}

    def open_database(self, path):
        return self._dbs[path]

    def _create_database(self, path):
        db = inmemory.InMemoryDatabase('db-%s' % path)
        self._dbs[path] = db
        return db


class TestCaseWithSyncServer(TestCase):

    def setUp(self):
        super(TestCaseWithSyncServer, self).setUp()
        self.server = self.server_thread = None

    def startServer(self, request_handler=sync_server.TCPSyncRequestHandler):
        self.request_state = ServerStateForTests()
        self.server = sync_server.TCPSyncServer(
            ('127.0.0.1', 0), request_handler,
            self.request_state)
        self.server_thread = threading.Thread(target=self.server.serve_forever,
                                              kwargs=dict(poll_interval=0.01))
        self.server_thread.start()
        self.addCleanup(self.server_thread.join)
        self.addCleanup(self.server.force_shutdown)

    def getURL(self, path=None):
        host, port = self.server.server_address
        if path is None:
            path = ''
        return 'u1db://%s:%s/%s' % (host, port, path)


def socket_pair():
    """Return a pair of TCP sockets connected to each other.

    Unlike socket.socketpair, this should work on Windows.
    """
    sock_pair = getattr(socket, 'socket_pair', None)
    if sock_pair:
        return sock_pair(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.bind(('127.0.0.1', 0))
    listen_sock.listen(1)
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.connect(listen_sock.getsockname())
    server_sock, addr = listen_sock.accept()
    listen_sock.close()
    return server_sock, client_sock



def load_with_scenarios(loader, standard_tests, pattern):
    """Load the tests in a given module.

    This just applies testscenarios.generate_scenarios to all the tests that
    are present. We do it at load time rather than at run time, because it
    plays nicer with various tools.
    """
    suite = loader.suiteClass()
    suite.addTests(testscenarios.generate_scenarios(standard_tests))
    return suite
