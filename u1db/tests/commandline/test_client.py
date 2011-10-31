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

import sys
import subprocess
import time

from u1db import (
    __version__ as _u1db_version,
    tests,
    )
from u1db.commandline import client


class TestArgs(tests.TestCase):

    def setUp(self):
        super(TestArgs, self).setUp()
        self.parser = client.setup_arg_parser()

    def parse_args(self, args):
        try:
            return self.parser.parse_args(args)
        except SystemExit, e:
            raise AssertionError('got SystemExit')

    def test_get(self):
        args = self.parse_args(['get', 'test.db', 'doc-id'])
        self.assertEqual(client.client_get, args.func)
        self.assertEqual('test.db', args.database)
        self.assertEqual('doc-id', args.doc_id)
        self.assertEqual(sys.stdout, args.outfile)

    def test_get_dash(self):
        args = self.parse_args(['get', 'test.db', 'doc-id', '-'])
        self.assertEqual(client.client_get, args.func)
        self.assertEqual('test.db', args.database)
        self.assertEqual('doc-id', args.doc_id)
        self.assertEqual(sys.stdout, args.outfile)

    def test_put(self):
        args = self.parse_args(['put', 'test.db', 'doc-id', 'old-doc-rev'])
        self.assertEqual(client.client_put, args.func)
        self.assertEqual('test.db', args.database)
        self.assertEqual('doc-id', args.doc_id)
        self.assertEqual('old-doc-rev', args.doc_rev)
        self.assertEqual(sys.stdin, args.infile)

    def test_sync(self):
        args = self.parse_args(['sync', 'source', 'target'])
        self.assertEqual(client.client_sync, args.func)
        self.assertEqual('source', args.source)
        self.assertEqual('target', args.target)
