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
    sync,
    )
from u1db.backends import sqlite_backend
from u1db.commandline import command
from u1db.remote import (
    client,
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
        db = sqlite_backend.SQLiteDatabase.open_database(database)
        doc_id, doc_rev = db.create_doc(infile.read(), doc_id=doc_id)
        self.stderr.write('id: %s\nrev: %s\n' % (doc_id, doc_rev))

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
        db = sqlite_backend.SQLiteDatabase.open_database(database)
        doc_rev, doc, has_conflicts = db.get_doc(doc_id)
        outfile.write(doc)
        self.stderr.write('rev: %s\n' % (doc_rev,))
        if has_conflicts:
            pass

client_commands.register(CmdGet)


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
        db = sqlite_backend.SQLiteDatabase.open_database(database)
        doc_rev = db.put_doc(doc_id, doc_rev, infile.read())
        self.stderr.write('rev: %s\n' % (doc_rev,))

client_commands.register(CmdPut)


class CmdSync(command.Command):
    """Synchronize two databases"""

    name = 'sync'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('source', help='database to sync from')
        parser.add_argument('target', help='database to sync to')

    def run(self, source, target):
        """Start a Sync request."""
        source_db = sqlite_backend.SQLiteDatabase.open_database(source)
        target_db = sqlite_backend.SQLiteDatabase.open_database(target)
        st = target_db.get_sync_target()
        syncer = sync.Synchronizer(source_db, st)
        syncer.sync()

client_commands.register(CmdSync)



def main(args):
    return client_commands.run_argv(args, sys.stdin, sys.stdout, sys.stderr)
