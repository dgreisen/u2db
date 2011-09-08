# Copyright (C) 2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""A U1DB implementation that uses SQLite as its persistence layer."""

import u1db
from u1db.vectorclock import VectorClockRev


class SQLiteDatabase(u1db.Database):
    """A U1DB implementation that uses SQLite as its persistence layer."""

    def __init__(self, sqlite_file):
        """Create a new sqlite file, identified by 
