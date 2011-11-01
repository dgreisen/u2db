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

import cStringIO
import os
import sys
import subprocess
import time

from u1db import (
    __version__ as _u1db_version,
    tests,
    )
from u1db.backends import (
    sqlite_backend,
    )
from u1db.commandline import client
from u1db.tests.commandline import safe_close


class TestArgs(tests.TestCase):

    def setUp(self):
        super(TestArgs, self).setUp()
        self.parser = client.setup_arg_parser()

    def parse_args(self, args):
        # ArgumentParser.parse_args doesn't play very nicely with a test suite,
        # so we trap SystemExit in case something is wrong with the args we're
        # parsing.
        try:
            return self.parser.parse_args(args)
        except SystemExit, e:
            raise AssertionError('got SystemExit')


    def test_create(self):
        args = self.parse_args(['create', 'test.db'])
        self.assertEqual(client.client_create, args.func)
        self.assertEqual('test.db', args.database)
        self.assertEqual(None, args.doc_id)
        self.assertEqual(sys.stdin, args.infile)

    def test_create_custom_doc_id(self):
        args = self.parse_args(['create', '--doc-id', 'xyz', 'test.db'])
        self.assertEqual(client.client_create, args.func)
        self.assertEqual('test.db', args.database)
        self.assertEqual('xyz', args.doc_id)
        self.assertEqual(sys.stdin, args.infile)

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

    def test_init_db(self):
        args = self.parse_args(['init-db', 'test.db'])
        self.assertEqual(client.client_init_db, args.func)
        self.assertEqual('test.db', args.database)

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


class TestCaseWithDB(tests.TestCase):

    def setUp(self):
        super(TestCaseWithDB, self).setUp()
        self.working_dir = self.createTempDir()
        self.db_path = self.working_dir + '/test.db'
        self.db = sqlite_backend.SQLitePartialExpandDatabase(self.db_path)
        self.db._set_replica_uid('test')


class TestCmdCreate(TestCaseWithDB):

    def test_create(self):
        out = cStringIO.StringIO()
        inf = cStringIO.StringIO(tests.simple_doc)
        client.cmd_create(self.db_path, 'test-id', inf, out)
        doc_rev, doc, has_conflicts = self.db.get_doc('test-id')
        self.assertEqual(tests.simple_doc, doc)
        self.assertFalse(has_conflicts)
        self.assertEqual('doc_id: test-id\ndoc_rev: %s\n' % (doc_rev,),
                         out.getvalue())


class TestCmdGet(TestCaseWithDB):

    def setUp(self):
        super(TestCmdGet, self).setUp()
        _, self.doc_rev = self.db.create_doc(tests.simple_doc,
                                             doc_id='my-test-doc')

    def test_get_simple(self):
        out = cStringIO.StringIO()
        err = cStringIO.StringIO()
        client.cmd_get(self.db_path, 'my-test-doc', out_file=out, err_file=err)
        self.assertEqual(tests.simple_doc, out.getvalue())
        self.assertEqual('doc_rev: %s\n' % (self.doc_rev,),
                         err.getvalue())


class TestCmdInit(TestCaseWithDB):

    def test_init_new(self):
        path = self.working_dir + '/test2.db'
        self.assertFalse(os.path.exists(path))
        client.cmd_init_db(path)
        self.assertTrue(os.path.exists(path))
        db = sqlite_backend.SQLiteDatabase.open_database(path)


class TestCmdPut(TestCaseWithDB):

    def setUp(self):
        super(TestCmdPut, self).setUp()
        _, self.doc_rev = self.db.create_doc(tests.simple_doc,
                                             doc_id='my-test-doc')

    def test_put_simple(self):
        inf = cStringIO.StringIO(tests.nested_doc)
        out = cStringIO.StringIO()
        client.cmd_put(self.db_path, 'my-test-doc', self.doc_rev,
                       inf, out)
        doc_rev, doc, has_conflicts = self.db.get_doc('my-test-doc')
        self.assertEqual(tests.nested_doc, doc)
        self.assertFalse(has_conflicts)
        self.assertEqual('doc_rev: %s\n' % (doc_rev,),
                         out.getvalue())


class TestCmdSync(TestCaseWithDB):

    def setUp(self):
        super(TestCmdSync, self).setUp()
        self.db2_path = self.working_dir + '/test2.db'
        self.db2 = sqlite_backend.SQLitePartialExpandDatabase(self.db2_path)
        self.db2._set_replica_uid('test2')
        _, self.doc_rev = self.db.create_doc(tests.simple_doc,
                                             doc_id='test-id')
        _, self.doc2_rev = self.db2.create_doc(tests.nested_doc,
                                               doc_id='my-test-id')

    def test_sync(self):
        client.cmd_sync(self.db_path, self.db2_path)
        self.assertEqual((self.doc_rev, tests.simple_doc, False),
                         self.db2.get_doc('test-id'))
        self.assertEqual((self.doc2_rev, tests.nested_doc, False),
                         self.db.get_doc('my-test-id'))


class TestCommandLine(TestCaseWithDB):

    def _get_u1db_client_path(self):
        from u1db import __path__ as u1db_path
        u1db_parent_dir = os.path.dirname(u1db_path[0])
        return os.path.join(u1db_parent_dir, 'u1db-client')

    def runU1DBClient(self, args):
        command = [sys.executable, self._get_u1db_client_path()]
        command.extend(args)
        p = subprocess.Popen(command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(safe_close, p)
        return p

    def run_main(self, args, stdin=None):
        if stdin is not None:
            self.patch(sys, 'stdin', cStringIO.StringIO(stdin))
        stdout = cStringIO.StringIO()
        stderr = cStringIO.StringIO()
        self.patch(sys, 'stdout', stdout)
        self.patch(sys, 'stderr', stderr)
        ret = client.main(args)
        if ret is None:
            ret = 0
        return ret, stdout.getvalue(), stderr.getvalue()

    def test_create_subprocess(self):
        p = self.runU1DBClient(['create', '--doc-id', 'test-id', self.db_path])
        stdout, stderr = p.communicate(tests.simple_doc)
        self.assertEqual(0, p.returncode)
        self.assertEqual('', stderr)
        doc_rev, doc, has_conflicts = self.db.get_doc('test-id')
        self.assertEqual(tests.simple_doc, doc)
        self.assertFalse(has_conflicts)
        self.assertEqual('doc_id: test-id\ndoc_rev: %s\n' % (doc_rev,),
                         stdout)

    def test_get(self):
        _, doc_rev = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        ret, stdout, stderr = self.run_main(['get', self.db_path, 'test-id'])
        self.assertEqual(0, ret)
        self.assertEqual(tests.simple_doc, stdout)
        self.assertEqual('doc_rev: %s\n' % (doc_rev,), stderr)

    def test_init(self):
        path = self.working_dir + '/test2.db'
        ret, stdout, stderr = self.run_main(['init-db', path])
        db2 = sqlite_backend.SQLiteDatabase.open_database(path)

    def test_put(self):
        _, doc_rev = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        ret, stdout, stderr = self.run_main(
            ['put', self.db_path, 'test-id', doc_rev],
            stdin=tests.nested_doc)
        doc_rev, doc, has_conflicts = self.db.get_doc('test-id')
        self.assertFalse(has_conflicts)
        self.assertEqual(tests.nested_doc, doc)
        self.assertEqual(0, ret)
        self.assertEqual('doc_rev: %s\n' % (doc_rev,), stdout)
        self.assertEqual('', stderr)

    def test_sync(self):
        _, doc_rev = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        self.db2_path = self.working_dir + '/test2.db'
        self.db2 = sqlite_backend.SQLitePartialExpandDatabase(self.db2_path)
        ret, stdout, stderr = self.run_main(
            ['sync', self.db_path, self.db2_path])
        self.assertEqual(0, ret)
        self.assertEqual('', stdout)
        self.assertEqual('', stderr)
        self.assertEqual((doc_rev, tests.simple_doc, False),
                         self.db2.get_doc('test-id'))
