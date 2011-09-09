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

"""A Cython wrapper around the C implementation of U1DB Database backend."""


cdef extern from "u1db.h":
    struct _u1database:
        pass
    ctypedef _u1database u1database
    u1database * u1db_create(char *fname)
    void u1db_free(u1database **)
    int u1db__sql_close(u1database *)
    int u1db__sql_is_open(u1database *)


cdef class CDatabase:
    """A thin wrapper/shim to interact with the C implementation.

    Functionality should not be written here. It is only provided as a way to
    expose the C API to the python test suite.
    """

    cdef public object _filename
    cdef u1database *_db

    def __init__(self, filename):
        self._filename = filename
        self._db = u1db_create(self._filename)

    def __dealloc__(self):
        u1db_free(&self._db)

    def _close_sqlite_handle(self):
        return u1db__sql_close(self._db)
        
    def _sql_is_open(self):
        if self._db == NULL:
            return True
        return u1db__sql_is_open(self._db)
