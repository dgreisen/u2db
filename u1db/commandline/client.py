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
from u1db.remote import (
    client,
    )


def cmd_create(database, doc_id, in_file, out_file, err_file):
    """Run 'create_doc'."""
    db = sqlite_backend.SQLiteDatabase.open_database(database)
    doc_id, doc_rev = db.create_doc(in_file.read(), doc_id=doc_id)
    err_file.write('id: %s\nrev: %s\n' % (doc_id, doc_rev))


def client_create(args):
    return cmd_create(args.database, args.doc_id, args.infile, sys.stdout,
                      sys.stderr)


def cmd_get(database, doc_id, out_file, err_file):
    """actually run 'get_doc' on the given parameters"""
    db = sqlite_backend.SQLiteDatabase.open_database(database)
    doc_rev, doc, has_conflicts = db.get_doc(doc_id)
    out_file.write(doc)
    err_file.write('rev: %s\n' % (doc_rev,))
    if has_conflicts:
        pass


def client_get(args):
    """Run 'get_doc' for this client"""
    return cmd_get(args.database, args.doc_id, args.outfile, sys.stderr)


def cmd_put(database, doc_id, old_doc_rev, in_file, out_file, err_file):
    """run 'put_doc' and update the data."""
    db = sqlite_backend.SQLiteDatabase.open_database(database)
    doc_rev = db.put_doc(doc_id, old_doc_rev, in_file.read())
    err_file.write('rev: %s\n' % (doc_rev,))


def client_put(args):
    """Run 'put_doc'"""
    return cmd_put(args.database, args.doc_id, args.doc_rev, args.infile,
                   sys.stdout, sys.stderr)


def cmd_sync(source_db, target_db):
    """Start a Sync request."""
    source = sqlite_backend.SQLiteDatabase.open_database(source_db)
    target = sqlite_backend.SQLiteDatabase.open_database(target_db)
    st = target.get_sync_target()
    syncer = sync.Synchronizer(source, st)
    syncer.sync()


def client_sync(args):
    """Run sync"""
    return cmd_sync(args.source, args.target)


def setup_arg_parser():
    p = argparse.ArgumentParser(description='Run actions from the U1DB client')
    p.add_argument('-V', '--version', action='version', version=_u1db_version)
    p.add_argument('-v', '--verbose', action='store_true', help='be chatty')
    subs = p.add_subparsers(title='commands')
    parser_create = subs.add_parser('create',
        help='Create a new document from scratch')
    parser_create.add_argument('database', help='The database to update')
    parser_create.add_argument('infile', nargs='?', default=sys.stdin,
        help='The file to read content from.')
    parser_create.add_argument('--doc-id', default=None,
        help='Set the document identifier')
    parser_create.set_defaults(func=client_create)
    parser_get = subs.add_parser('get',
        help='Extract a document from the database')
    parser_get.add_argument('database', help='The database to query')
    parser_get.add_argument('doc_id', help='The document id to retrieve.')
    parser_get.add_argument('outfile', nargs='?', default=sys.stdout,
        help='The file to write the document to',
        type=argparse.FileType('wb'))
    parser_get.set_defaults(func=client_get)
    parser_put = subs.add_parser('put', help='Add a document to the database')
    parser_put.add_argument('database', help='The database to update')
    parser_put.add_argument('doc_id', help='The document id to retrieve')
    parser_put.add_argument('doc_rev',
        help='The revision of the document (which is being superseded.)')
    parser_put.add_argument('infile', nargs='?', default=sys.stdin,
        help='The filename of the document that will be used for content',
        type=argparse.FileType('rb'))
    parser_put.set_defaults(func=client_put)
    parser_sync = subs.add_parser('sync', help='Synchronize two databases')
    parser_sync.add_argument('source', help='database to sync from')
    parser_sync.add_argument('target', help='database to sync to')
    parser_sync.set_defaults(func=client_sync)
    return p


def main(args):
    p = setup_arg_parser()
    args = p.parse_args(args)
    return args.func(args)
