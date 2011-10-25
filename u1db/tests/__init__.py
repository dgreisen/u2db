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

import socket

import testscenarios
import testtools

from u1db.backends import (
    inmemory,
    sqlite_backend,
    )

TestCase = testtools.TestCase


simple_doc = '{"key": "value"}'
nested_doc = '{"key": "value", "sub": {"doc": "underneath"}}'


def create_memory_database(machine_id):
    return inmemory.InMemoryDatabase(machine_id)


def create_sqlite_expanded(machine_id):
    db = sqlite_backend.SQLiteExpandedDatabase(':memory:')
    db._set_machine_id(machine_id)
    return db


def create_sqlite_partial_expanded(machine_id):
    db = sqlite_backend.SQLitePartialExpandDatabase(':memory:')
    db._set_machine_id(machine_id)
    return db


def create_sqlite_only_expanded(machine_id):
    db = sqlite_backend.SQLiteOnlyExpandedDatabase(':memory:')
    db._set_machine_id(machine_id)
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
