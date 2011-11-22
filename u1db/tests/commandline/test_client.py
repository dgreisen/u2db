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
from u1db.tests import test_remote_sync_target


class TestArgs(tests.TestCase):

    def setUp(self):
        super(TestArgs, self).setUp()
        self.parser = client.client_commands.make_argparser()

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
        self.assertEqual(client.CmdCreate, args.subcommand)
        self.assertEqual('test.db', args.database)
        self.assertEqual(None, args.doc_id)
        self.assertEqual(None, args.infile)

    def test_create_custom_doc_id(self):
        args = self.parse_args(['create', '--id', 'xyz', 'test.db'])
        self.assertEqual(client.CmdCreate, args.subcommand)
        self.assertEqual('test.db', args.database)
        self.assertEqual('xyz', args.doc_id)
        self.assertEqual(None, args.infile)

    def test_get(self):
        args = self.parse_args(['get', 'test.db', 'doc-id'])
        self.assertEqual(client.CmdGet, args.subcommand)
        self.assertEqual('test.db', args.database)
        self.assertEqual('doc-id', args.doc_id)
        self.assertEqual(None, args.outfile)

    def test_get_dash(self):
        args = self.parse_args(['get', 'test.db', 'doc-id', '-'])
        self.assertEqual(client.CmdGet, args.subcommand)
        self.assertEqual('test.db', args.database)
        self.assertEqual('doc-id', args.doc_id)
        self.assertEqual(sys.stdout, args.outfile)

    def test_init_db(self):
        args = self.parse_args(['init-db', 'test.db', 'replica-uid'])
        self.assertEqual(client.CmdInitDB, args.subcommand)
        self.assertEqual('test.db', args.database)
        self.assertEqual('replica-uid', args.replica_uid)

    def test_put(self):
        args = self.parse_args(['put', 'test.db', 'doc-id', 'old-doc-rev'])
        self.assertEqual(client.CmdPut, args.subcommand)
        self.assertEqual('test.db', args.database)
        self.assertEqual('doc-id', args.doc_id)
        self.assertEqual('old-doc-rev', args.doc_rev)
        self.assertEqual(None, args.infile)

    def test_sync(self):
        args = self.parse_args(['sync', 'source', 'target'])
        self.assertEqual(client.CmdSync, args.subcommand)
        self.assertEqual('source', args.source)
        self.assertEqual('target', args.target)


class TestCaseWithDB(tests.TestCase):

    def setUp(self):
        super(TestCaseWithDB, self).setUp()
        self.working_dir = self.createTempDir()
        self.db_path = self.working_dir + '/test.db'
        self.db = sqlite_backend.SQLitePartialExpandDatabase(self.db_path)
        self.db._set_replica_uid('test')
        self.addCleanup(self.db.close)

    def make_command(self, cls, stdin_content=''):
        inf = cStringIO.StringIO(stdin_content)
        out = cStringIO.StringIO()
        err = cStringIO.StringIO()
        return cls(inf, out, err)


class TestCmdCreate(TestCaseWithDB):

    def test_create(self):
        cmd = self.make_command(client.CmdCreate)
        inf = cStringIO.StringIO(tests.simple_doc)
        cmd.run(self.db_path, inf, 'test-id')
        doc = self.db.get_doc('test-id')
        self.assertEqual(tests.simple_doc, doc.content)
        self.assertFalse(doc.has_conflicts)
        self.assertEqual('', cmd.stdout.getvalue())
        self.assertEqual('id: test-id\nrev: %s\n' % (doc.rev,),
                         cmd.stderr.getvalue())


class TestCmdGet(TestCaseWithDB):

    def setUp(self):
        super(TestCmdGet, self).setUp()
        self.doc = self.db.create_doc(tests.simple_doc, doc_id='my-test-doc')

    def test_get_simple(self):
        cmd = self.make_command(client.CmdGet)
        cmd.run(self.db_path, 'my-test-doc', None)
        self.assertEqual(tests.simple_doc, cmd.stdout.getvalue())
        self.assertEqual('rev: %s\n' % (self.doc.rev,),
                         cmd.stderr.getvalue())


class TestCmdInit(TestCaseWithDB):

    def test_init_new(self):
        path = self.working_dir + '/test2.db'
        self.assertFalse(os.path.exists(path))
        cmd = self.make_command(client.CmdInitDB)
        cmd.run(path, 'test-uid')
        self.assertTrue(os.path.exists(path))
        db = sqlite_backend.SQLiteDatabase.open_database(path)
        self.assertEqual('test-uid', db._replica_uid)


class TestCmdPut(TestCaseWithDB):

    def setUp(self):
        super(TestCmdPut, self).setUp()
        self.doc = self.db.create_doc(tests.simple_doc, doc_id='my-test-doc')

    def test_put_simple(self):
        cmd = self.make_command(client.CmdPut)
        inf = cStringIO.StringIO(tests.nested_doc)
        cmd.run(self.db_path, 'my-test-doc', self.doc.rev, inf)
        doc = self.db.get_doc('my-test-doc')
        self.assertNotEqual(self.doc.rev, doc.rev)
        self.assertGetDoc(self.db, 'my-test-doc', doc.rev,
                          tests.nested_doc, False)
        self.assertEqual('', cmd.stdout.getvalue())
        self.assertEqual('rev: %s\n' % (doc.rev,),
                         cmd.stderr.getvalue())


class TestCmdSync(TestCaseWithDB):

    def setUp(self):
        super(TestCmdSync, self).setUp()
        self.db2_path = self.working_dir + '/test2.db'
        self.db2 = sqlite_backend.SQLitePartialExpandDatabase(self.db2_path)
        self.addCleanup(self.db2.close)
        self.db2._set_replica_uid('test2')
        self.doc  = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        self.doc2 = self.db2.create_doc(tests.nested_doc, doc_id='my-test-id')

    def test_sync(self):
        cmd = self.make_command(client.CmdSync)
        cmd.run(self.db_path, self.db2_path)
        self.assertGetDoc(self.db2, 'test-id', self.doc.rev, tests.simple_doc,
                          False)
        self.assertGetDoc(self.db, 'my-test-id', self.doc2.rev,
                          tests.nested_doc, False)


class TestCmdSyncRemote(tests.TestCaseWithServer, TestCaseWithDB):

    @staticmethod
    def server_def():
        return test_remote_sync_target.http_server_def()

    def setUp(self):
        super(TestCmdSyncRemote, self).setUp()
        self.startServer()
        self.db2 = self.request_state._create_database('test2.db')

    def test_sync_remote(self):
        doc1 = self.db.create_doc(tests.simple_doc)
        doc2 = self.db2.create_doc(tests.nested_doc)
        db2_url = self.getURL('test2.db')
        self.assertTrue(db2_url.startswith('http://'))
        self.assertTrue(db2_url.endswith('/test2.db'))
        cmd = self.make_command(client.CmdSync)
        cmd.run(self.db_path, db2_url)
        self.assertGetDoc(self.db2, doc1.doc_id, doc1.rev, tests.simple_doc,
                          False)
        self.assertGetDoc(self.db, doc2.doc_id, doc2.rev, tests.nested_doc,
                          False)


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
        p = self.runU1DBClient(['create', '--id', 'test-id', self.db_path])
        stdout, stderr = p.communicate(tests.simple_doc)
        self.assertEqual(0, p.returncode)
        self.assertEqual('', stdout)
        doc = self.db.get_doc('test-id')
        self.assertEqual(tests.simple_doc, doc.content)
        self.assertFalse(doc.has_conflicts)
        self.assertEqual('id: test-id\nrev: %s\n' % (doc.rev,),
                         stderr.replace('\r\n', '\n'))

    def test_get(self):
        doc = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        ret, stdout, stderr = self.run_main(['get', self.db_path, 'test-id'])
        self.assertEqual(0, ret)
        self.assertEqual(tests.simple_doc, stdout)
        self.assertEqual('rev: %s\n' % (doc.rev,), stderr)

    def test_init_db(self):
        path = self.working_dir + '/test2.db'
        ret, stdout, stderr = self.run_main(['init-db', path, 'uid'])
        db2 = sqlite_backend.SQLiteDatabase.open_database(path)

    def test_put(self):
        doc = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        ret, stdout, stderr = self.run_main(
            ['put', self.db_path, 'test-id', doc.rev],
            stdin=tests.nested_doc)
        doc = self.db.get_doc('test-id')
        self.assertFalse(doc.has_conflicts)
        self.assertEqual(tests.nested_doc, doc.content)
        self.assertEqual(0, ret)
        self.assertEqual('', stdout)
        self.assertEqual('rev: %s\n' % (doc.rev,), stderr)

    def test_sync(self):
        doc = self.db.create_doc(tests.simple_doc, doc_id='test-id')
        self.db2_path = self.working_dir + '/test2.db'
        self.db2 = sqlite_backend.SQLitePartialExpandDatabase(self.db2_path)
        self.addCleanup(self.db2.close)
        ret, stdout, stderr = self.run_main(
            ['sync', self.db_path, self.db2_path])
        self.assertEqual(0, ret)
        self.assertEqual('', stdout)
        self.assertEqual('', stderr)
        self.assertGetDoc(self.db2, 'test-id', doc.rev, tests.simple_doc, False)