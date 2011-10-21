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


class InvalidDocRev(U1DBError):
    """The document revisions supplied does not match the current version."""


class InvalidDocId(U1DBError):
    """A document was tried with an invalid document identifier."""


class ConflictedDoc(U1DBError):
    """The document is conflicted, you must call resolve before put()"""


class InvalidValueForIndex(U1DBError):
    """The values supplied does not match the index definition.

    Can also be raised if wildcard matches are not strictly at the tail of the
    request.
    """


class BadProtocolStream(U1DBError):
    """Raised when part of the protocol stream is incorrectly formatted."""


class UnknownProtocolVersion(BadProtocolStream):
    """Raised when the protocol header is unknown to us."""


class UnknownRequest(BadProtocolStream):
    """Raised when an RPC comes in for a request we don't know about."""
