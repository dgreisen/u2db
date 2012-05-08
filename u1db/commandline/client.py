# Copyright 2011 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

"""Commandline bindings for the u1db-client program."""

import argparse
import os
import sys

from u1db import (
    Document,
    open as u1db_open,
    sync,
    )
from u1db.commandline import command
from u1db.remote import (
    http_database,
    http_target,
    )


client_commands = command.CommandGroup()


def set_oauth_credentials(client):
    keys = os.environ.get('OAUTH_CREDENTIALS', None)
    if keys is not None:
        consumer_key, consumer_secret, \
            token_key, token_secret = keys.split(":")
        client.set_oauth_credentials(consumer_key, consumer_secret,
                      token_key, token_secret)


class OneDbCmd(command.Command):
    """Base class for commands operating on one local or remote database."""

    def _open(self, database, create):
        if database.startswith(('http://', 'https://')):
            db = http_database.HTTPDatabase(database)
            set_oauth_credentials(db)
            db.open(create)
            return db
        else:
            return u1db_open(database, create)


class CmdCreate(OneDbCmd):
    """Create a new document from scratch"""

    name = 'create'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database',
                            help='The local or remote database to update',
                            metavar='database-path-or-url')
        parser.add_argument('infile', nargs='?', default=None,
            help='The file to read content from.')
        parser.add_argument('--id', dest='doc_id', default=None,
            help='Set the document identifier')

    def run(self, database, infile, doc_id):
        if infile is None:
            infile = self.stdin
        db = self._open(database, create=False)
        doc = db.create_doc(infile.read(), doc_id=doc_id)
        self.stderr.write('id: %s\nrev: %s\n' % (doc.doc_id, doc.rev))

client_commands.register(CmdCreate)


class CmdDelete(OneDbCmd):
    """Delete a document from the database"""

    name = 'delete'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database',
                            help='The local or remote database to update',
                            metavar='database-path-or-url')
        parser.add_argument('doc_id', help='The document id to retrieve')
        parser.add_argument('doc_rev',
            help='The revision of the document (which is being superseded.)')

    def run(self, database, doc_id, doc_rev):
        db = self._open(database, create=False)
        doc = Document(doc_id, doc_rev, None)
        db.delete_doc(doc)
        self.stderr.write('rev: %s\n' % (doc.rev,))

client_commands.register(CmdDelete)


class CmdGet(OneDbCmd):
    """Extract a document from the database"""

    name = 'get'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database',
                            help='The local or remote database to query',
                            metavar='database-path-or-url')
        parser.add_argument('doc_id', help='The document id to retrieve.')
        parser.add_argument('outfile', nargs='?', default=None,
            help='The file to write the document to',
            type=argparse.FileType('wb'))

    def run(self, database, doc_id, outfile):
        if outfile is None:
            outfile = self.stdout
        db = self._open(database, create=False)
        doc = db.get_doc(doc_id)
        if doc is None:
            self.stderr.write('Document not found (id: %s)\n' % (doc_id,))
            return 1  # failed
        if doc.content is None:
            outfile.write('[contents deleted]\n')
        else:
            outfile.write(doc.content)
        self.stderr.write('rev: %s\n' % (doc.rev,))
        if doc.has_conflicts:
            # TODO: Probably want to write 'conflicts' or 'conflicted' to
            # stderr.
            pass

client_commands.register(CmdGet)


class CmdInitDB(OneDbCmd):
    """Create a new database"""

    name = 'init-db'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database',
                            help='The local or remote database to create',
                            metavar='database-path-or-url')
        parser.add_argument('--replica-uid', default=None,
            help='The unique identifier for this database (not for remote)')

    def run(self, database, replica_uid):
        db = self._open(database, create=True)
        if replica_uid is not None:
            db._set_replica_uid(replica_uid)

client_commands.register(CmdInitDB)


class CmdPut(OneDbCmd):
    """Add a document to the database"""

    name = 'put'

    @classmethod
    def _populate_subparser(cls, parser):
        parser.add_argument('database',
                            help='The local or remote database to update',
                            metavar='database-path-or-url'),
        parser.add_argument('doc_id', help='The document id to retrieve')
        parser.add_argument('doc_rev',
            help='The revision of the document (which is being superseded.)')
        parser.add_argument('infile', nargs='?', default=None,
            help='The filename of the document that will be used for content',
            type=argparse.FileType('rb'))

    def run(self, database, doc_id, doc_rev, infile):
        if infile is None:
            infile = self.stdin
        db = self._open(database, create=False)
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
        if target.startswith(('http://', 'https://')):
            st = http_target.HTTPSyncTarget.connect(target)
            set_oauth_credentials(st)
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
