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

"""A list of errors that u1db can raise."""


class U1DBError(Exception):
    """Generic base class for U1DB errors."""

    wire_description = "error"

    def __init__(self, message=None):
        self.message = message


class RevisionConflict(U1DBError):
    """The document revisions supplied does not match the current version."""

    wire_description = "revision conflict"


class InvalidDocId(U1DBError):
    """A document was tried with an invalid document identifier."""


class ConflictedDoc(U1DBError):
    """The document is conflicted, you must call resolve before put()"""


class InvalidValueForIndex(U1DBError):
    """The values supplied does not match the index definition.

    Can also be raised if wildcard matches are not strictly at the tail of the
    request.
    """


class DatabaseDoesNotExist(U1DBError):
    """The database does not exist."""


class HTTPError(U1DBError):
    """Unspecific HTTP errror."""

    wire_description = None

    def __init__(self, status, message=None):
        self.status = status
        self.message = message


wire_description_to_exc = dict((x.wire_description, x)
                                   for x in globals().values()
                                   if isinstance(x, type) and
                                      issubclass(x, U1DBError) and
                                      x.wire_description)
