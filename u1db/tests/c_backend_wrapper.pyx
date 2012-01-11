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

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t n)

cdef extern from "u1db/u1db.h":
    ctypedef struct u1database:
        pass
    ctypedef struct u1db_document:
        char *doc_id
        size_t doc_id_len
        char *doc_rev
        size_t doc_rev_len
        char *content
        size_t content_len
        int has_conflicts

    ctypedef struct u1db_row:
        u1db_row *next
        int num_columns
        int *column_sizes
        unsigned char **columns

    ctypedef struct u1db_table:
        int status
        u1db_row *first_row

    ctypedef struct u1db_record:
        u1db_record *next
        char *doc_id
        char *doc_rev
        char *doc

    ctypedef struct u1db_vectorclock_item:
        char *machine_id
        int db_rev

    ctypedef struct u1db_vectorclock:
        int num_items
        u1db_vectorclock_item *items

    u1database * u1db_open(char *fname)
    void u1db_free(u1database **)
    int u1db_set_machine_id(u1database *, char *machine_id)
    int u1db_get_machine_id(u1database *, char **machine_id)
    int u1db__get_db_rev(u1database *, int *db_rev)
    char *u1db__allocate_doc_id(u1database *)
    int u1db__sql_close(u1database *)
    int u1db__sql_is_open(u1database *)
    u1db_table *u1db__sql_run(u1database *, char *sql, size_t n)
    void u1db__free_table(u1db_table **table)
    void *calloc(size_t, size_t)
    void free(void *)
    int u1db_create_doc(u1database *db, char *doc, size_t n,
                        char **doc_id, char **doc_rev)
    int u1db_put_doc(u1database *db, char *doc_id, char **doc_rev,
                     char *doc, int n)
    int u1db_get_doc(u1database *db, char *doc_id, char **doc_rev,
                     char **doc, int *n, int *has_conflicts)
    int u1db_delete_doc(u1database *db, char *doc_id, char **doc_rev)
    int u1db_whats_changed(u1database *db, int *db_rev,
                           int (*cb)(void *, char *doc_id), void *context)
    int u1db__sync_get_machine_info(u1database *db, char *other_machine_id,
                                    int *other_db_rev, char **my_machine_id,
                                    int *my_db_rev)
    int u1db__sync_record_machine_info(u1database *db, char *machine_id,
                                       int db_rev)
    int u1db__sync_exchange(u1database *db, char *from_machine_id,
                            int from_db_rev, int last_known_rev,
                            u1db_record *from_records, u1db_record **new_records,
                            u1db_record **conflict_records)
    u1db_record *u1db__create_record(char *doc_id, char *doc_rev, char *doc)
    void u1db__free_records(u1db_record **)

    u1db_vectorclock *u1db__vectorclock_from_str(char *s)
    void u1db__free_vectorclock(u1db_vectorclock **clock)
    int u1db__vectorclock_increment(u1db_vectorclock *clock, char *machine_id)
    int u1db__vectorclock_maximize(u1db_vectorclock *clock,
                                   u1db_vectorclock *other)
    int u1db__vectorclock_as_str(u1db_vectorclock *clock, char **result)
    int u1db__vectorclock_is_newer(u1db_vectorclock *maybe_newer,
                                   u1db_vectorclock *older)

    int U1DB_OK
    int U1DB_INVALID_DOC_REV
    int U1DB_INVALID_DOC_ID

    u1db_document *u1db_make_doc(char *doc_id, int doc_id_len,
                                 char *revision, int revision_len,
                                 char *content, int content_len,
                                 int has_conflicts)
    void u1db_free_doc(u1db_document **doc)

import u1db

cdef int _add_to_set(void *context, char *doc_id):
    a_set = <object>(context)
    doc = doc_id
    a_set.add(doc)


cdef int _append_to_list(void *context, char *doc_id):
    a_list = <object>context
    doc = doc_id
    a_list.append(doc)


cdef class CDocument(object):
    """A thin wrapper around the C Document struct."""

    cdef u1db_document *_doc

    def __init__(self, doc_id, rev, content, has_conflicts=False):
        cdef int conflict

        if has_conflicts:
            conflict = 1
        else:
            conflict = 0
        self._doc = u1db_make_doc(doc_id, len(doc_id),
                                  rev, len(rev), content, len(content),
                                  conflict)

    def __dealloc__(self):
        u1db_free_doc(&self._doc)

    property doc_id:
        def __get__(self):
            return PyString_FromStringAndSize(
                    self._doc.doc_id, self._doc.doc_id_len)

    property rev:
        def __get__(self):
            return PyString_FromStringAndSize(
                    self._doc.doc_rev, self._doc.doc_rev_len)

    property content:
        def __get__(self):
            return PyString_FromStringAndSize(
                    self._doc.content, self._doc.content_len)

    property has_conflicts:
        def __get__(self):
            if self._doc.has_conflicts:
                return True
            return False

    def __repr__(self):
        if self._doc.has_conflicts:
            extra = ', conflicted'
        else:
            extra = ''
        return '%s(%s, %s%s, %r)' % (self.__class__.__name__, self.doc_id,
                                     self.rev, extra, self.content)

    def __hash__(self):
        raise NotImplementedError(self.__hash__)

    def __richcmp__(self, other, int t):
        if t == 0: # Py_LT <
            return ((self.doc_id, self.rev, self.content)
                < (other.doc_id, other.rev, other.content))
        elif t == 2: # Py_EQ ==
            return (self.doc_id == other.doc_id
                    and self.rev == other.rev
                    and self.content == other.content
                    and self.has_conflicts == other.has_conflicts)

        return NotImplemented


cdef class CDatabase(object):
    """A thin wrapper/shim to interact with the C implementation.

    Functionality should not be written here. It is only provided as a way to
    expose the C API to the python test suite.
    """

    cdef public object _filename
    cdef u1database *_db
    cdef public object _supports_indexes
    cdef public object _last_exchange_log

    def __init__(self, filename):
        self._supports_indexes = False
        self._filename = filename
        self._db = u1db_open(self._filename)

    def __dealloc__(self):
        u1db_free(&self._db)

    def _close_sqlite_handle(self):
        return u1db__sql_close(self._db)

    def _sql_is_open(self):
        if self._db == NULL:
            return True
        return u1db__sql_is_open(self._db)

    property _machine_id:
        def __get__(self):
            cdef char * val
            cdef int status
            status = u1db_get_machine_id(self._db, &val)
            if status != 0:
                if val != NULL:
                    err = str(val)
                else:
                    err = "<unknown>"
                raise RuntimeError("Failed to get_machine_id: %d %s"
                                   % (status, err))
            if val == NULL:
                return None
            return str(val)

    def _set_machine_id(self, machine_id):
        cdef int status
        status = u1db_set_machine_id(self._db, machine_id)
        if status != 0:
            raise RuntimeError('Machine_id could not be set to %s, error: %d'
                               % (machine_id, status))

    def _allocate_doc_id(self):
        cdef char *val
        val = u1db__allocate_doc_id(self._db)
        if val == NULL:
            raise RuntimeError("Failed to allocate document id")
        s = str(val)
        free(val)
        return s

    def _get_db_rev(self):
        cdef int db_rev, status

        status = u1db__get_db_rev(self._db, &db_rev)
        if status != 0:
            raise RuntimeError('Failed to _get_db_rev: %d' % (status,))
        return db_rev

    def _run_sql(self, sql):
        cdef u1db_table *tbl
        cdef u1db_row *cur_row
        cdef size_t n
        cdef int i

        if self._db == NULL:
            raise RuntimeError("called _run_sql with a NULL pointer.")
        tbl = u1db__sql_run(self._db, sql, len(sql))
        if tbl == NULL:
            raise MemoryError("Failed to allocate table memory.")
        try:
            if tbl.status != 0:
                raise RuntimeError("Status was not 0: %d" % (tbl.status,))
            # Now convert the table into python
            res = []
            cur_row = tbl.first_row
            while cur_row != NULL:
                row = []
                for i from 0 <= i < cur_row.num_columns:
                    row.append(PyString_FromStringAndSize(
                        <char*>(cur_row.columns[i]), cur_row.column_sizes[i]))
                res.append(tuple(row))
                cur_row = cur_row.next
            return res
        finally:
            u1db__free_table(&tbl)

    def create_doc(self, doc, doc_id=None):
        cdef int status
        cdef char *c_doc_id, *c_doc_rev

        if doc_id is not None:
            c_doc_id = doc_id
        else:
            c_doc_id = NULL
        c_doc_rev = NULL
        status = u1db_create_doc(self._db, doc, len(doc),
                                 &c_doc_id, &c_doc_rev)
        if status != 0:
            if status == U1DB_INVALID_DOC_REV:
                raise u1db.InvalidDocRev()
            raise RuntimeError('Failed to create_doc: %d' % (status,))
        # TODO: Handle the free() calls
        if c_doc_id == NULL:
            doc_id = None
        elif doc_id is None:
            doc_id = c_doc_id
            free(c_doc_id)
        if c_doc_rev == NULL:
            doc_rev = None
        else:
            doc_rev = c_doc_rev
        return doc_id, doc_rev

    def put_doc(self, doc_id, doc_rev, doc):
        cdef int status
        cdef char *c_doc_rev
        cdef char *c_doc_id

        if doc_rev is None:
            c_doc_rev = NULL
        else:
            c_doc_rev = doc_rev
        if doc_id is None:
            c_doc_id = NULL
        else:
            c_doc_id = doc_id
        status = u1db_put_doc(self._db, c_doc_id, &c_doc_rev, doc, len(doc))
        if status == 0:
            doc_rev = c_doc_rev
            free(c_doc_rev)
        else:
            if status == U1DB_INVALID_DOC_REV:
                raise u1db.InvalidDocRev()
            if status == U1DB_INVALID_DOC_ID:
                raise u1db.InvalidDocId()
            raise RuntimeError("Failed to put_doc: %d" % (status,))
        return doc_rev

    def get_doc(self, doc_id):
        cdef int status, n, c_has_conflicts
        cdef char *c_doc_rev, *c_doc

        c_doc_rev = c_doc = NULL
        c_has_conflicts = n = status = 0
        status = u1db_get_doc(self._db, doc_id, &c_doc_rev, &c_doc, &n,
                              &c_has_conflicts)
        if status != 0:
            raise RuntimeError("Failed to get_doc: %d" % (status,))
        if c_has_conflicts:
            has_conflicts = True
        else:
            has_conflicts = False
        if c_doc == NULL:
            doc = None
        else:
            doc = c_doc
            free(c_doc)
        if c_doc_rev == NULL:
            doc_rev = None
        else:
            doc_rev = c_doc_rev
            free(c_doc_rev)
        return doc_rev, doc, has_conflicts

    def delete_doc(self, doc_id, doc_rev):
        cdef int status
        cdef char *c_doc_rev

        c_doc_rev = doc_rev;
        status = u1db_delete_doc(self._db, doc_id, &c_doc_rev);
        if status != U1DB_OK:
            if status == U1DB_INVALID_DOC_REV:
                raise u1db.InvalidDocRev("Failed to delete %s %s, %s"
                                         % (doc_id, doc_rev, c_doc_rev))
            elif status == U1DB_INVALID_DOC_ID:
                raise KeyError
            raise RuntimeError("Failed to delete_doc: %d" % (status,))
        doc_rev = c_doc_rev
        return doc_rev

    def whats_changed(self, db_rev=0):
        cdef int status, c_db_rev

        a_set = set()
        c_db_rev = db_rev
        status = u1db_whats_changed(self._db, &c_db_rev, _add_to_set, <void*>a_set)
        if status != 0:
            raise RuntimeError("Failed to call whats_changed: %d" % (status,))
        return c_db_rev, a_set

    def _get_transaction_log(self):
        cdef int status, c_db_rev

        c_db_rev = 0;
        # For now, whats_changed does a callback for every item, so we can use
        # it to inspect the transaction log.
        a_list = []
        status = u1db_whats_changed(self._db, &c_db_rev, _append_to_list,
                                    <void*>a_list)
        if status != 0:
            raise RuntimeError("Failed to call whats_changed: %d" % (status,))
        return a_list

    def _get_sync_info(self, other_machine_id):
        cdef int status, my_db_rev, other_db_rev
        cdef char *my_machine_id

        status = u1db__sync_get_machine_info(self._db, other_machine_id,
                                             &other_db_rev, &my_machine_id,
                                             &my_db_rev)
        if status != U1DB_OK:
            raise RuntimeError("Failed to _get_sync_info: %d" % (status,))
        return (my_machine_id, my_db_rev, other_db_rev)

    def _record_sync_info(self, machine_id, db_rev):
        cdef int status

        status = u1db__sync_record_machine_info(self._db, machine_id, db_rev)
        if status != U1DB_OK:
            raise RuntimeError("Failed to _record_sync_info: %d" % (status,))

    def _sync_exchange(self, docs_info, from_machine_id, from_machine_rev,
                       last_known_rev):
        cdef int status
        cdef u1db_record *from_records, *next_record
        cdef u1db_record *new_records, *conflict_records

        from_records = next_record = NULL
        for doc_id, doc_rev, doc in reversed(docs_info):
            next_record = u1db__create_record(doc_id, doc_rev, doc)
            next_record.next = from_records
            from_records = next_record
        new_records = conflict_records = NULL
        status = u1db__sync_exchange(self._db, from_machine_id,
            from_machine_rev, last_known_rev,
            from_records, &new_records, &conflict_records)
        u1db__free_records(&from_records)
        if status != U1DB_OK:
            raise RuntimeError("Failed to _sync_exchange: %d" % (status,))


cdef class VectorClockRev:

    cdef u1db_vectorclock *_clock

    def __init__(self, s):
        if s is None:
            self._clock = u1db__vectorclock_from_str(NULL)
        else:
            self._clock = u1db__vectorclock_from_str(s)

    def __dealloc__(self):
        u1db__free_vectorclock(&self._clock)

    def __repr__(self):
        cdef int status
        cdef char *res
        if self._clock == NULL:
            return '%s(None)' % (self.__class__.__name__,)
        status = u1db__vectorclock_as_str(self._clock, &res)
        if status != U1DB_OK:
            return '%s(<failure: %d>)' % (status,)
        return '%s(%s)' % (self.__class__.__name__, res)

    def as_dict(self):
        cdef u1db_vectorclock *cur
        cdef int i
        if self._clock == NULL:
            return None
        res = {}
        for i from 0 <= i < self._clock.num_items:
            res[self._clock.items[i].machine_id] = self._clock.items[i].db_rev
        return res

    def as_str(self):
        cdef int status
        cdef char *res

        status = u1db__vectorclock_as_str(self._clock, &res)
        if status != U1DB_OK:
            raise RuntimeError("Failed to VectorClockRev.as_str(): %d" % (status,))
        if res == NULL:
            s = None
        else:
            s = res
            free(res)
        return s

    def increment(self, machine_id):
        cdef int status

        status = u1db__vectorclock_increment(self._clock, machine_id)
        if status != U1DB_OK:
            raise RuntimeError("Failed to increment: %d" % (status,))

    def maximize(self, vcr):
        cdef int status
        cdef VectorClockRev other

        other = vcr
        status = u1db__vectorclock_maximize(self._clock, other._clock)
        if status != U1DB_OK:
            raise RuntimeError("Failed to maximize: %d" % (status,))

    def is_newer(self, vcr):
        cdef int is_newer
        cdef VectorClockRev other

        other = vcr
        is_newer = u1db__vectorclock_is_newer(self._clock, other._clock)
        if is_newer == 0:
            return False
        elif is_newer == 1:
            return True
        else:
            raise RuntimeError("Failed to is_newer: %d" % (is_newer,))
