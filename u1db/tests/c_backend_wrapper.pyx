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
    int PyString_AsStringAndSize(object o, char **buf, Py_ssize_t *length
                                 ) except -1

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
    int u1db_create_doc(u1database *db, char *content, char *doc_id,
                        u1db_document **doc)
    int u1db_delete_doc(u1database *db, u1db_document *doc)
    int u1db_get_doc(u1database *db, char *doc_id, u1db_document **doc)
    int u1db_put_doc(u1database *db, u1db_document *doc)
    int u1db_delete_doc(u1database *db, u1db_document *doc)
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
    int U1DB_INVALID_PARAMETER
    int U1DB_REVISION_CONFLICT
    int U1DB_INVALID_DOC_ID
    int U1DB_DOCUMENT_ALREADY_DELETED
    int U1DB_DOCUMENT_DOES_NOT_EXIST

    u1db_document *u1db_make_doc(char *doc_id, char *revision, char *content,
                                 int has_conflicts)
    void u1db_free_doc(u1db_document **doc)
    int u1db_doc_set_content(u1db_document *doc, char *content)

from u1db import errors

cdef int _add_to_set(void *context, char *doc_id):
    a_set = <object>(context)
    doc = doc_id
    a_set.add(doc)


cdef int _append_to_list(void *context, char *doc_id):
    a_list = <object>context
    doc = doc_id
    a_list.append(doc)


def make_document(doc_id, rev, content, has_conflicts=False):
    cdef u1db_document *doc
    cdef char *c_content, *c_rev, *c_doc_id
    cdef int conflict

    if has_conflicts:
        conflict = 1
    else:
        conflict = 0
    if doc_id is None:
        c_doc_id = NULL
    else:
        c_doc_id = doc_id
    if content is None:
        c_content = NULL
    else:
        c_content = content
    if rev is None:
        c_rev = NULL
    else:
        c_rev = rev
    doc = u1db_make_doc(c_doc_id, c_rev, c_content, conflict)
    pydoc = CDocument()
    pydoc._doc = doc
    return pydoc


cdef class CDocument(object):
    """A thin wrapper around the C Document struct."""

    cdef u1db_document *_doc

    def __init__(self):
        self._doc = NULL

    def __dealloc__(self):
        u1db_free_doc(&self._doc)

    property doc_id:
        def __get__(self):
            if self._doc.doc_id == NULL:
                return None
            return PyString_FromStringAndSize(
                    self._doc.doc_id, self._doc.doc_id_len)

    property rev:
        def __get__(self):
            if self._doc.doc_rev == NULL:
                return None
            return PyString_FromStringAndSize(
                    self._doc.doc_rev, self._doc.doc_rev_len)

    property content:
        def __get__(self):
            if self._doc.content == NULL:
                return None
            return PyString_FromStringAndSize(
                    self._doc.content, self._doc.content_len)

        def __set__(self, val):
            u1db_doc_set_content(self._doc, val)


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


cdef handle_status(int status, context):
    if status == U1DB_OK:
        return
    if status == U1DB_REVISION_CONFLICT:
        raise errors.RevisionConflict()
    if status == U1DB_INVALID_DOC_ID:
        raise errors.InvalidDocId()
    if status == U1DB_DOCUMENT_ALREADY_DELETED:
        raise errors.DocumentAlreadyDeleted()
    if status == U1DB_DOCUMENT_DOES_NOT_EXIST:
        raise errors.DocumentDoesNotExist()
    if status == U1DB_INVALID_PARAMETER:
        raise RuntimeError('Bad parameters supplied')
    raise RuntimeError('%s (status: %s)' % (context, status))


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

    def close(self):
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

    def create_doc(self, content, doc_id=None):
        cdef u1db_document *doc = NULL
        cdef int status
        cdef char *c_doc_id, *c_doc_rev

        if doc_id is None:
            c_doc_id = NULL
        else:
            c_doc_id = doc_id
        c_doc_rev = NULL
        status = u1db_create_doc(self._db, content, c_doc_id, &doc)
        handle_status(status, 'Failed to create_doc')
        pydoc = CDocument()
        pydoc._doc = doc
        return pydoc

    def put_doc(self, CDocument doc):
        cdef int status

        handle_status(u1db_put_doc(self._db, doc._doc),
                      "Failed to put_doc")
        return doc.rev

    def get_doc(self, doc_id):
        cdef int status
        cdef u1db_document *doc = NULL

        status = u1db_get_doc(self._db, doc_id, &doc)
        handle_status(status, "get_doc failed")
        if doc == NULL:
            return None
        pydoc = CDocument()
        pydoc._doc = doc
        return pydoc

    def delete_doc(self, CDocument doc):
        cdef int status

        status = u1db_delete_doc(self._db, doc._doc);
        handle_status(status, "Failed to delete %s" % (doc,))

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
