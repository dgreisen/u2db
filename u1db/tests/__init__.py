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

from u1db import (
    Document,
    )
from u1db.backends import (
    inmemory,
    sqlite_backend,
    )
from u1db.remote import (
    server_state,
    )


# Setting this means that failing assertions will not include this module in
# their traceback. However testtools doesn't seem to set it, and we don't want
# this level to be omitted, but the lower levels to be shown.
# __unittest = 1

class TestCase(testtools.TestCase):

    def createTempDir(self, prefix='u1db-tmp-'):
        """Create a temporary directory to do some work in.

        This directory will be scheduled for cleanup when the test ends.
        """
        tempdir = tempfile.mkdtemp(prefix=prefix)
        self.addCleanup(shutil.rmtree, tempdir)
        return tempdir

    def assertGetDoc(self, db, doc_id, doc_rev, content, has_conflicts):
        """Assert that the document in the database looks correct."""
        exp_doc = Document(doc_id, doc_rev, content,
                           has_conflicts=has_conflicts)
        self.assertEqual(exp_doc, db.get_doc(doc_id))

    def assertGetDocConflicts(self, db, doc_id, conflicts):
        """Assert what conflicts are stored for a given doc_id.

        :param conflicts: A list of (doc_Rev, content) pairs.
            The first item must match the first item returned from the
            database, however the rest can be returned in any order.
        """
        if conflicts:
            conflicts = conflicts[:1] + sorted(conflicts[1:])
        actual = db.get_doc_conflicts(doc_id)
        if actual:
            actual = actual[:1] + sorted(actual[1:])
        self.assertEqual(conflicts, actual)


def multiply_scenarios(a_scenarios, b_scenarios):
    """Create the cross-product of scenarios."""

    all_scenarios = []
    for a_name, a_attrs in a_scenarios:
        for b_name, b_attrs in b_scenarios:
            name = '%s,%s' % (a_name, b_name)
            attrs = dict(a_attrs)
            attrs.update(b_attrs)
            all_scenarios.append((name, attrs))
    return all_scenarios


simple_doc = '{"key": "value"}'
nested_doc = '{"key": "value", "sub": {"doc": "underneath"}}'


def create_memory_database(test, replica_uid):
    return inmemory.InMemoryDatabase(replica_uid)


def create_sqlite_partial_expanded(test, replica_uid):
    db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
    db._set_replica_uid(replica_uid)
    return db


LOCAL_DATABASES_SCENARIOS = [
        ('mem', {'do_create_database': create_memory_database}),
        ('sql', {'do_create_database': create_sqlite_partial_expanded}),
        ]


class DatabaseBaseTests(TestCase):

    create_database = None
    scenarios = LOCAL_DATABASES_SCENARIOS

    def create_database(self, replica_uid):
        return self.do_create_database(self, replica_uid)

    def setUp(self):
        super(DatabaseBaseTests, self).setUp()
        self.db = self.create_database('test')

    def tearDown(self):
        # TODO: Add close_database parameterization
        # self.close_database(self.db)
        super(DatabaseBaseTests, self).tearDown()


class ServerStateForTests(server_state.ServerState):
    """Used in the test suite, so we don't have to touch disk, etc."""

    def __init__(self):
        super(ServerStateForTests, self).__init__()
        self._dbs = {}

    def open_database(self, path):
        return self._dbs[path]

    def _create_database(self, path):
        db = inmemory.InMemoryDatabase(path)
        self._dbs[path] = db
        return db


class ResponderForTests(object):
    """Responder for tests."""
    _started = False
    sent_response = False
    status = None

    def start_response(self, status='success', **kwargs):
        self._started = True
        self.status = status
        self.kwargs = kwargs

    def send_response(self, status='success', **kwargs):
        self.start_response(status, **kwargs)
        self.finish_response()

    def finish_response(self):
        self.sent_response = True


class TestCaseWithServer(TestCase):

    @staticmethod
    def server_def():
        # should return (ServerClass, RequestHandlerClass,
        #                "shutdown method name", "url_scheme")
        raise NotImplementedError(TestCaseWithServer.server_def)

    def setUp(self):
        super(TestCaseWithServer, self).setUp()
        self.server = self.server_thread = None

    @property
    def url_scheme(self):
        return self.server_def()[-1]

    def startServer(self, other_request_handler=None):
        server_def = self.server_def()
        server_class, request_handler, shutdown_meth, _ = server_def
        request_handler = other_request_handler or request_handler
        self.request_state = ServerStateForTests()
        self.server = server_class(('127.0.0.1', 0), request_handler,
                                   self.request_state)
        self.server_thread = threading.Thread(target=self.server.serve_forever,
                                              kwargs=dict(poll_interval=0.01))
        self.server_thread.start()
        self.addCleanup(self.server_thread.join)
        self.addCleanup(getattr(self.server, shutdown_meth))

    def getURL(self, path=None):
        host, port = self.server.server_address
        if path is None:
            path = ''
        return '%s://%s:%s/%s' % (self.url_scheme, host, port, path)


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
