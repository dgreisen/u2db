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
    )
from u1db.remote import (
    client,
    )


def client_get(args):
    """Run 'get' for this client"""


def client_put(args):
    """Run 'put'"""


def client_sync(args):
    """Run sync"""


def setup_arg_parser():
    p = argparse.ArgumentParser(description='Run actions from the U1DB client')
    p.add_argument('-V', '--version', action='version', version=_u1db_version)
    p.add_argument('-v', '--verbose', action='store_true', help='be chatty')
    subs = p.add_subparsers(title='subcommands')
    parser_get = subs.add_parser('get',
        help='Extract a document from the database')
    parser_get.add_argument('database', help='The database to query')
    parser_get.add_argument('doc_id', help='The document id to retrieve.')
    parser_get.add_argument('outfile', nargs='?', default=sys.stdout,
        help='The file to write the document to',
        type=argparse.FileType('wb'))
    parser_get.set_defaults(func=client_get)
    parser_put = subs.add_parser('put', help='Add a document to the database')
    parser_put.add_argument('database', help='The database to query')
    parser_put.add_argument('doc_id', help='The document id to retrieve.')
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
