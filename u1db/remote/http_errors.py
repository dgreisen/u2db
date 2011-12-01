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

"""Information about the encoding of errors over HTTP."""

from u1db import (
    errors,
    )


# error wire descriptions mapping to HTTP status codes
wire_description_to_status = dict([
    (errors.InvalidDocId.wire_description, 400),
    (errors.DatabaseDoesNotExist.wire_description, 404),
    (errors.DocumentDoesNotExist.wire_description, 404),
    (errors.DocumentAlreadyDeleted.wire_description, 404),
    (errors.RevisionConflict.wire_description, 409),
# without matching exception
    (errors.DOCUMENT_DELETED, 404)
])


# 400 included for explicitly for tests
ERROR_STATUSES = set(wire_description_to_status.values())|set([400])
