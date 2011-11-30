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

"""Commandline bindings for the u1db-client program."""

import argparse
import sys

from u1db import (
    __version__ as _u1db_version,
    Document,
    open as u1db_open,
    sync,
    )
from u1db.backends import sqlite_backend
from u1db.commandline import command
from u1db.remote import (
    http_target,
    )


client_commands = command.CommandGroup()


class CmdCreate(command.Command):
    """Create a new document from scratch"""

    name = 'create'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database', help='The database to update')
        parser.add_argument('infile', nargs='?', default=None,
            help='The file to read content from.')
        parser.add_argument('--id', dest='doc_id', default=None,
            help='Set the document identifier')

    def run(self, database, infile, doc_id):
        if infile is None:
            infile = self.stdin
        db = u1db_open(database, create=False)
        doc = db.create_doc(infile.read(), doc_id=doc_id)
        self.stderr.write('id: %s\nrev: %s\n' % (doc.doc_id, doc.rev))

client_commands.register(CmdCreate)


class CmdGet(command.Command):
    """Extract a document from the database"""

    name = 'get'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database', help='The database to query')
        parser.add_argument('doc_id', help='The document id to retrieve.')
        parser.add_argument('outfile', nargs='?', default=None,
            help='The file to write the document to',
            type=argparse.FileType('wb'))

    def run(self, database, doc_id, outfile):
        if outfile is None:
            outfile = self.stdout
        db = u1db_open(database, create=False)
        doc = db.get_doc(doc_id)
        outfile.write(doc.content)
        self.stderr.write('rev: %s\n' % (doc.rev,))
        if doc.has_conflicts:
            # TODO: Probably want to write 'conflicts' or 'conflicted' to
            # stderr.
            pass

client_commands.register(CmdGet)


class CmdInitDB(command.Command):
    """Create a new database"""

    name = 'init-db'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database', help='The database to create')
        parser.add_argument('replica_uid',
            help='The unique identifier for this database')

    def run(self, database, replica_uid):
        db = u1db_open(database, create=True)
        db._set_replica_uid(replica_uid)

client_commands.register(CmdInitDB)


class CmdPut(command.Command):
    """Add a document to the database"""

    name = 'put'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database', help='The database to update')
        parser.add_argument('doc_id', help='The document id to retrieve')
        parser.add_argument('doc_rev',
            help='The revision of the document (which is being superseded.)')
        parser.add_argument('infile', nargs='?', default=None,
            help='The filename of the document that will be used for content',
            type=argparse.FileType('rb'))

    def run(self, database, doc_id, doc_rev, infile):
        if infile is None:
            infile = self.stdin
        db = u1db_open(database, create=False)
        doc = Document(doc_id, doc_rev, infile.read())
        doc_rev = db.put_doc(doc)
        self.stderr.write('rev: %s\n' % (doc_rev,))

client_commands.register(CmdPut)


class CmdSync(command.Command):
    """Synchronize two databases"""

    name = 'sync'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('source', help='database to sync from')
        parser.add_argument('target', help='database to sync to')

    def _open_target(self, target):
        if target.startswith('http://'):
            st = http_target.HTTPSyncTarget.connect(target)
        else:
            db = u1db_open(target, create=True)
            st = db.get_sync_target()
        return st

    def run(self, source, target):
        """Start a Sync request."""
        source_db = u1db_open(source, create=False)
        st = self._open_target(target)
        syncer = sync.Synchronizer(source_db, st)
        syncer.sync()
        source_db.close()

client_commands.register(CmdSync)


def main(args):
    return client_commands.run_argv(args, sys.stdin, sys.stdout, sys.stderr)
