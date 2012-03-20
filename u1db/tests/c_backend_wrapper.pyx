# Copyright 2011-2012 Canonical Ltd.
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
    char *PyString_AsString(object) except NULL
    char *PyString_AS_STRING(object)
    char *strdup(char *)
    void *calloc(size_t, size_t)
    void free(void *)
    ctypedef struct FILE:
        pass
    fprintf(FILE *, char *, ...)
    FILE *stderr
    size_t strlen(char *)

cdef extern from "stdarg.h":
    ctypedef struct va_list:
        pass
    void va_start(va_list, void*)
    void va_start_int "va_start" (va_list, int)
    void va_end(va_list)

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
    # Note: u1query is actually defined in u1db_internal.h, and in u1db.h it is
    #       just an opaque pointer. However, older versions of Cython don't let
    #       you have a forward declaration and a full declaration, so we just
    #       expose the whole thing here.
    ctypedef struct u1query:
        char *index_name
        int num_fields
        char **fields

    ctypedef char* const_char_ptr "const char*"
    ctypedef int (*u1db_doc_callback)(void *context, u1db_document *doc)
    ctypedef int (*u1db_doc_gen_callback)(void *context,
        u1db_document *doc, int gen)
    ctypedef int (*u1db_doc_id_gen_callback)(void *context,
        const_char_ptr doc_id, int gen)

    u1database * u1db_open(char *fname)
    void u1db_free(u1database **)
    int u1db_set_replica_uid(u1database *, char *replica_uid)
    int u1db_get_replica_uid(u1database *, const_char_ptr *replica_uid)
    int u1db_create_doc(u1database *db, char *content, char *doc_id,
                        u1db_document **doc)
    int u1db_delete_doc(u1database *db, u1db_document *doc)
    int u1db_get_doc(u1database *db, char *doc_id, u1db_document **doc)
    int u1db_get_docs(u1database *db, int n_doc_ids, const_char_ptr *doc_ids,
                      int check_for_conflicts, void *context,
                      u1db_doc_callback cb)
    int u1db_put_doc(u1database *db, u1db_document *doc)
    int u1db_put_doc_if_newer(u1database *db, u1db_document *doc,
                              int save_conflict, char *replica_uid,
                              int replica_gen, int *state)
    int u1db_resolve_doc(u1database *db, u1db_document *doc,
                         int n_revs, const_char_ptr *revs)
    int u1db_delete_doc(u1database *db, u1db_document *doc)
    int u1db_whats_changed(u1database *db, int *gen, void *context,
                           u1db_doc_id_gen_callback cb)
    int u1db__get_transaction_log(u1database *db, void *context,
                                  u1db_doc_id_gen_callback cb)
    int u1db_get_doc_conflicts(u1database *db, char *doc_id, void *context,
                               u1db_doc_callback cb)

    int u1db_create_index(u1database *db, char *index_name,
                          int n_expressions, const_char_ptr *expressions)
    int u1db_delete_index(u1database *db, char *index_name)

    int u1db_list_indexes(u1database *db, void *context,
                  int (*cb)(void *context, const_char_ptr index_name,
                            int n_expressions, const_char_ptr *expressions))
    int u1db_get_from_index(u1database *db, u1query *query, void *context,
                            u1db_doc_callback cb, int n_values, char *val0, ...)
    int u1db_simple_lookup1(u1database *db, char *index_name, char *val1,
                            void *context, u1db_doc_callback cb)

    int u1db_query_init(u1database *db, char *index_name, u1query **query)
    void u1db_free_query(u1query **query)

    int U1DB_OK
    int U1DB_INVALID_PARAMETER
    int U1DB_REVISION_CONFLICT
    int U1DB_INVALID_DOC_ID
    int U1DB_DOCUMENT_ALREADY_DELETED
    int U1DB_DOCUMENT_DOES_NOT_EXIST
    int U1DB_NOT_IMPLEMENTED
    int U1DB_INVALID_JSON
    int U1DB_INVALID_VALUE_FOR_INDEX
    int U1DB_BROKEN_SYNC_STREAM
    int U1DB_INTERNAL_ERROR

    int U1DB_INSERTED
    int U1DB_SUPERSEDED
    int U1DB_CONVERGED
    int U1DB_CONFLICTED

    void u1db_free_doc(u1db_document **doc)
    int u1db_doc_set_content(u1db_document *doc, char *content)


cdef extern from "u1db/u1db_internal.h":
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

    ctypedef struct u1db_sync_exchange:
        int target_gen
        int num_doc_ids
        char **doc_ids_to_return
        int *gen_for_doc_ids

    ctypedef int (*u1db__trace_callback)(void *context, const_char_ptr state)
    ctypedef struct u1db_sync_target:
        int (*get_sync_info)(u1db_sync_target *st,
            char *source_replica_uid,
            const_char_ptr *st_replica_uid, int *st_gen, int *source_gen) nogil
        int (*record_sync_info)(u1db_sync_target *st,
            char *source_replica_uid, int source_gen) nogil
        int (*sync_exchange_docs)(u1db_sync_target *st,
                                  char *source_replica_uid, int n_docs,
                                  u1db_document **docs, int *generations,
                                  int *target_gen, void *context,
                                  u1db_doc_gen_callback cb) nogil
        int (*sync_exchange)(u1db_sync_target *st, u1database *source_db,
                int n_doc_ids, const_char_ptr *doc_ids, int *generations,
                int *target_gen,
                void *context, u1db_doc_gen_callback cb) nogil
        int (*get_sync_exchange)(u1db_sync_target *st,
                                 char *source_replica_uid,
                                 int last_known_source_gen,
                                 u1db_sync_exchange **exchange) nogil
        void (*finalize_sync_exchange)(u1db_sync_target *st,
                                       u1db_sync_exchange **exchange) nogil
        int (*_set_trace_hook)(u1db_sync_target *st,
                               void *context, u1db__trace_callback cb) nogil


    int u1db__get_generation(u1database *, int *db_rev)
    char *u1db__allocate_doc_id(u1database *)
    int u1db__sql_close(u1database *)
    int u1db__sql_is_open(u1database *)
    u1db_table *u1db__sql_run(u1database *, char *sql, size_t n)
    void u1db__free_table(u1db_table **table)
    u1db_record *u1db__create_record(char *doc_id, char *doc_rev, char *doc)
    void u1db__free_records(u1db_record **)

    u1db_document *u1db__allocate_document(char *doc_id, char *revision,
                                           char *content, int has_conflicts)
    int u1db__generate_hex_uuid(char *)

    int u1db__get_sync_generation(u1database *db, char *replica_uid,
                                  int *generation)
    int u1db__set_sync_generation(u1database *db, char *replica_uid,
                                  int generation)
    int u1db__sync_get_machine_info(u1database *db, char *other_replica_uid,
                                    int *other_db_rev, char **my_replica_uid,
                                    int *my_db_rev)
    int u1db__sync_record_machine_info(u1database *db, char *replica_uid,
                                       int db_rev)
    int u1db__sync_exchange(u1database *db, char *from_replica_uid,
                            int from_db_rev, int last_known_rev,
                            u1db_record *from_records, u1db_record **new_records,
                            u1db_record **conflict_records)
    int u1db__sync_exchange_seen_ids(u1db_sync_exchange *se, int *n_ids,
                                     const_char_ptr **doc_ids)
    int u1db__format_query(int n_fields, va_list argp, char **buf, int *wildcard)
    int u1db__get_sync_target(u1database *db, u1db_sync_target **sync_target)
    int u1db__free_sync_target(u1db_sync_target **sync_target)
    int u1db__sync_db_to_target(u1database *db, u1db_sync_target *target,
                                int *local_gen_before_sync)

    int u1db__sync_exchange_insert_doc_from_source(u1db_sync_exchange *se,
            u1db_document *doc, int source_gen)
    int u1db__sync_exchange_find_doc_ids_to_return(u1db_sync_exchange *se)
    int u1db__sync_exchange_return_docs(u1db_sync_exchange *se, void *context,
            int (*cb)(void *context, u1db_document *doc, int gen))
    int u1db__create_http_sync_target(char *url, u1db_sync_target **target)

cdef extern from "u1db/u1db_http_internal.h":
    int u1db__format_sync_url(u1db_sync_target *st,
            const_char_ptr source_replica_uid, char **sync_url)


cdef extern from "u1db/u1db_vectorclock.h":
    ctypedef struct u1db_vectorclock_item:
        char *replica_uid
        int generation

    ctypedef struct u1db_vectorclock:
        int num_items
        u1db_vectorclock_item *items

    u1db_vectorclock *u1db__vectorclock_from_str(char *s)
    void u1db__free_vectorclock(u1db_vectorclock **clock)
    int u1db__vectorclock_increment(u1db_vectorclock *clock, char *replica_uid)
    int u1db__vectorclock_maximize(u1db_vectorclock *clock,
                                   u1db_vectorclock *other)
    int u1db__vectorclock_as_str(u1db_vectorclock *clock, char **result)
    int u1db__vectorclock_is_newer(u1db_vectorclock *maybe_newer,
                                   u1db_vectorclock *older)

from u1db import errors
from sqlite3 import dbapi2


cdef int _append_doc_gen_to_list(void *context, const_char_ptr doc_id,
                                 int generation) with gil:
    a_list = <object>(context)
    doc = doc_id
    a_list.append((doc, generation))
    return 0


cdef int _append_doc_to_list(void *context, u1db_document *doc) with gil:
    a_list = <object>context
    pydoc = CDocument()
    pydoc._doc = doc
    a_list.append(pydoc)
    return 0


cdef _list_to_array(lst, const_char_ptr **res, int *count):
    cdef const_char_ptr *tmp
    count[0] = len(lst)
    tmp = <const_char_ptr*>calloc(sizeof(char*), count[0])
    for idx, x in enumerate(lst):
        tmp[idx] = x
    res[0] = tmp


cdef int _append_index_definition_to_list(void *context, 
        const_char_ptr index_name, int n_expressions,
        const_char_ptr *expressions) with gil:
    cdef int i

    a_list = <object>(context)
    exp_list = []
    for i from 0 <= i < n_expressions:
        exp_list.append(expressions[i])
    a_list.append((index_name, exp_list))
    return 0


cdef int _format_query_dotted(char **buf, int *wildcard, int n_fields, ...):
    cdef va_list argp
    cdef int status

    va_start_int(argp, n_fields)
    status = u1db__format_query(n_fields, argp, buf, wildcard)
    va_end(argp)
    return status


cdef int return_doc_cb_wrapper(void *context, u1db_document *doc,
        int gen) with gil:
    cdef CDocument pydoc
    user_cb = <object>context
    pydoc = CDocument()
    pydoc._doc = doc
    try:
        user_cb(pydoc, gen)
    except Exception, e:
        # We suppress the exception here, because intermediating through the C
        # layer gets a bit crazy
        return U1DB_INVALID_PARAMETER
    return U1DB_OK


cdef int _trace_hook(void *context, const_char_ptr state) with gil:
    if context == NULL:
        return U1DB_INVALID_PARAMETER
    ctx = <object>context
    try:
        ctx(state)
    except:
        # Note: It would be nice if we could map the Python exception into
        #       something in C
        return U1DB_INTERNAL_ERROR
    return U1DB_OK


def _format_query(fields):
    """Wrapper around u1db__format_query for testing."""
    cdef int status
    cdef char *buf
    cdef int wildcard[10]

    if len(fields) == 0:
        status = _format_query_dotted(&buf, wildcard, 0)
    elif len(fields) == 1:
        status = _format_query_dotted(&buf, wildcard, 1, <char*>fields[0])
    elif len(fields) == 2:
        status = _format_query_dotted(&buf, wildcard, 2, <char*>fields[0],
                <char*>fields[1])
    elif len(fields) == 3:
        status = _format_query_dotted(&buf, wildcard, 3, <char*>fields[0], 
                <char*>fields[1], <char*>fields[2])
    elif len(fields) == 4:
        status = _format_query_dotted(&buf, wildcard, 4, <char*>fields[0], 
                <char*>fields[1], <char*>fields[2], <char *>fields[3])
    else:
        status = U1DB_NOT_IMPLEMENTED
    handle_status("format_query", status)
    if buf == NULL:
        res = None
    else:
        res = buf
        free(buf)
    w = []
    for i in range(len(fields)):
        w.append(wildcard[i])
    return res, w


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
    doc = u1db__allocate_document(c_doc_id, c_rev, c_content, conflict)
    pydoc = CDocument()
    pydoc._doc = doc
    return pydoc


def generate_hex_uuid():
    uuid = PyString_FromStringAndSize(NULL, 32)
    handle_status("Failed to generate uuid",
        u1db__generate_hex_uuid(PyString_AS_STRING(uuid)))
    return uuid


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
        try:
            if t == 0: # Py_LT <
                return ((self.doc_id, self.rev, self.content)
                    < (other.doc_id, other.rev, other.content))
            elif t == 2: # Py_EQ ==
                return (self.doc_id == other.doc_id
                        and self.rev == other.rev
                        and self.content == other.content
                        and self.has_conflicts == other.has_conflicts)
        except AttributeError:
            # Fall through to NotImplemented
            pass

        return NotImplemented


cdef object safe_str(const_char_ptr s):
    if s == NULL:
        return None
    return s


cdef class CQuery:
    
    cdef u1query *_query

    def __init__(self):
        self._query = NULL

    def __dealloc__(self):
        u1db_free_query(&self._query)

    def _check(self):
        if self._query == NULL:
            raise RuntimeError("No valid _query.")

    property index_name:
        def __get__(self):
            self._check()
            return safe_str(self._query.index_name)

    property num_fields:
        def __get__(self):
            self._check()
            return self._query.num_fields

    property fields:
        def __get__(self):
            cdef int i
            self._check()
            fields = []
            for i from 0 <= i < self._query.num_fields:
                fields.append(safe_str(self._query.fields[i]))
            return fields


cdef handle_status(context, int status):
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
    if status == U1DB_NOT_IMPLEMENTED:
        raise NotImplementedError("Functionality not implemented yet: %s"
                                  % (context,))
    if status == U1DB_INVALID_VALUE_FOR_INDEX:
        raise errors.InvalidValueForIndex()
    if status == U1DB_INTERNAL_ERROR:
        raise errors.U1DBError("internal error")
    if status == U1DB_BROKEN_SYNC_STREAM:
        raise errors.BrokenSyncStream()
    if status == U1DB_CONFLICTED:
        raise errors.ConflictedDoc()
    raise RuntimeError('%s (status: %s)' % (context, status))


cdef class CDatabase
cdef class CSyncTarget

cdef class CSyncExchange(object):

    cdef u1db_sync_exchange *_exchange
    cdef CSyncTarget _target

    def __init__(self, CSyncTarget target, source_replica_uid, source_gen):
        self._target = target
        assert self._target._st.get_sync_exchange != NULL, \
                "get_sync_exchange is NULL?"
        handle_status("get_sync_exchange",
            self._target._st.get_sync_exchange(self._target._st,
                source_replica_uid, source_gen, &self._exchange))

    def __dealloc__(self):
        if self._target is not None and self._target._st != NULL:
            self._target._st.finalize_sync_exchange(self._target._st,
                    &self._exchange)

    def _check(self):
        if self._exchange == NULL:
            raise RuntimeError("self._exchange is NULL")

    property target_gen:
        def __get__(self):
            self._check()
            return self._exchange.target_gen

    def insert_doc_from_source(self, CDocument doc, source_gen):
        self._check()
        handle_status("sync_exchange",
            u1db__sync_exchange_insert_doc_from_source(self._exchange,
                doc._doc, source_gen))

    def find_doc_ids_to_return(self):
        self._check()
        handle_status("find_doc_ids_to_return",
            u1db__sync_exchange_find_doc_ids_to_return(self._exchange))

    def return_docs(self, return_doc_cb):
        self._check()
        handle_status("return_docs",
            u1db__sync_exchange_return_docs(self._exchange,
                <void *>return_doc_cb, &return_doc_cb_wrapper))

    def get_seen_ids(self):
        cdef const_char_ptr *seen_ids
        cdef int i, n_ids
        self._check()
        handle_status("sync_exchange_seen_ids",
            u1db__sync_exchange_seen_ids(self._exchange, &n_ids, &seen_ids))
        res = []
        for i from 0 <= i < n_ids:
            res.append(seen_ids[i])
        if (seen_ids != NULL):
            free(<void*>seen_ids)
        return res

    def get_doc_ids_to_return(self):
        self._check()
        res = []
        if (self._exchange.num_doc_ids > 0
                and self._exchange.doc_ids_to_return != NULL):
            for i from 0 <= i < self._exchange.num_doc_ids:
                res.append((self._exchange.doc_ids_to_return[i],
                            self._exchange.gen_for_doc_ids[i]))
        return res


cdef class CSyncTarget(object):

    cdef u1db_sync_target *_st
    cdef CDatabase _db

    def __init__(self):
        self._db = None
        self._st = NULL

    def __dealloc__(self):
        u1db__free_sync_target(&self._st)

    def _check(self):
        if self._st == NULL:
            raise RuntimeError("self._st is NULL")

    def get_sync_info(self, source_replica_uid):
        cdef const_char_ptr st_replica_uid = NULL
        cdef int st_gen = 0, source_gen = 0, status

        self._check()
        assert self._st.get_sync_info != NULL, "get_sync_info is NULL?"
        with nogil:
            status = self._st.get_sync_info(self._st, source_replica_uid,
                &st_replica_uid, &st_gen, &source_gen)
        handle_status("get_sync_info", status)
        return (safe_str(st_replica_uid), st_gen, source_gen)

    def record_sync_info(self, source_replica_uid, source_gen):
        cdef int status
        self._check()
        assert self._st.record_sync_info != NULL, "record_sync_info is NULL?"
        with nogil:
            status = self._st.record_sync_info(self._st, source_replica_uid,
                                               source_gen)
        handle_status("record_sync_info", status)

    def _get_sync_exchange(self, source_replica_uid, source_gen):
        self._check()
        return CSyncExchange(self, source_replica_uid, source_gen)

    def sync_exchange_doc_ids(self, source_db, doc_id_generations,
                              last_known_generation, return_doc_cb):
        cdef const_char_ptr *doc_ids
        cdef int *generations
        cdef int num_doc_ids
        cdef int target_gen
        cdef int status
        cdef CDatabase sdb

        self._check()
        assert self._st.sync_exchange != NULL, "sync_exchange is NULL?"
        sdb = source_db
        num_doc_ids = len(doc_id_generations)
        doc_ids = <const_char_ptr *>calloc(num_doc_ids, sizeof(char *))
        if doc_ids == NULL:
            raise MemoryError
        generations = <int *>calloc(num_doc_ids, sizeof(int))
        if generations == NULL:
            free(<void *>doc_ids)
            raise MemoryError
        try:
            for i, (doc_id, gen) in enumerate(doc_id_generations):
                doc_ids[i] = PyString_AsString(doc_id)
                generations[i] = gen
            target_gen = last_known_generation
            with nogil:
                status = self._st.sync_exchange(self._st, sdb._db,
                    num_doc_ids, doc_ids, generations, &target_gen,
                    <void*>return_doc_cb, return_doc_cb_wrapper)
            handle_status("sync_exchange", status)
        finally:
            free(<void *>doc_ids)
            free(generations)

        return target_gen

    def sync_exchange(self, docs_by_generations, source_replica_uid,
                      last_known_generation, return_doc_cb):
        cdef CDocument cur_doc
        cdef u1db_document **docs = NULL
        cdef int *generations = NULL
        cdef int i, count, status, target_gen

        self._check()
        assert self._st.sync_exchange_docs != NULL, "sync_exchange_docs is NULL?"
        count = len(docs_by_generations)
        try:
            docs = <u1db_document **>calloc(count, sizeof(u1db_document*))
            if docs == NULL:
                raise MemoryError
            generations = <int*>calloc(count, sizeof(int))
            if generations == NULL:
                raise MemoryError
            for i from 0 <= i < count:
                cur_doc = docs_by_generations[i][0]
                generations[i] = docs_by_generations[i][1]
                docs[i] = cur_doc._doc
            target_gen = last_known_generation
            with nogil:
                status = self._st.sync_exchange_docs(self._st,
                        source_replica_uid, count,
                        docs, generations, &target_gen, <void *>return_doc_cb,
                        return_doc_cb_wrapper)
            handle_status("sync_exchange_docs", status)
        finally:
            if docs != NULL:
                free(docs)
            if generations != NULL:
                free(generations)
        return target_gen

    def _set_trace_hook(self, cb):
        self._check()
        assert self._st._set_trace_hook != NULL, "_set_trace_hook is NULL?"
        handle_status("_set_trace_hook",
            self._st._set_trace_hook(self._st, <void*>cb, _trace_hook))


cdef class CDatabase(object):
    """A thin wrapper/shim to interact with the C implementation.

    Functionality should not be written here. It is only provided as a way to
    expose the C API to the python test suite.
    """

    cdef public object _filename
    cdef u1database *_db
    cdef public object _supports_indexes

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

    property _replica_uid:
        def __get__(self):
            cdef const_char_ptr val
            cdef int status
            status = u1db_get_replica_uid(self._db, &val)
            if status != 0:
                if val != NULL:
                    err = str(val)
                else:
                    err = "<unknown>"
                raise RuntimeError("Failed to get_replica_uid: %d %s"
                                   % (status, err))
            if val == NULL:
                return None
            return str(val)

    def _set_replica_uid(self, replica_uid):
        cdef int status
        status = u1db_set_replica_uid(self._db, replica_uid)
        if status != 0:
            raise RuntimeError('Machine_id could not be set to %s, error: %d'
                               % (replica_uid, status))

    def _allocate_doc_id(self):
        cdef char *val
        val = u1db__allocate_doc_id(self._db)
        if val == NULL:
            raise RuntimeError("Failed to allocate document id")
        s = str(val)
        free(val)
        return s

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
        cdef char *c_doc_id

        if doc_id is None:
            c_doc_id = NULL
        else:
            c_doc_id = doc_id
        handle_status('Failed to create_doc',
            u1db_create_doc(self._db, content, c_doc_id, &doc))
        pydoc = CDocument()
        pydoc._doc = doc
        return pydoc

    def put_doc(self, CDocument doc):
        handle_status("Failed to put_doc",
            u1db_put_doc(self._db, doc._doc))
        return doc.rev

    def put_doc_if_newer(self, CDocument doc, save_conflict, replica_uid=None,
                         replica_gen=None):
        cdef char *c_uid
        cdef int gen, state = 0

        if replica_uid is None:
            c_uid = NULL
        else:
            c_uid = replica_uid
        if replica_gen is None:
            gen = 0
        else:
            gen = replica_gen
        handle_status("Failed to put_doc_if_newer",
            u1db_put_doc_if_newer(self._db, doc._doc, save_conflict,
                c_uid, gen, &state)) 
        if state == U1DB_INSERTED:
            return 'inserted'
        elif state == U1DB_SUPERSEDED:
            return 'superseded'
        elif state == U1DB_CONVERGED:
            return 'converged'
        elif state == U1DB_CONFLICTED:
            return 'conflicted'
        else:
            raise RuntimeError("Unknown put_doc_if_newer state: %d" % (state,))

    def get_doc(self, doc_id):
        cdef u1db_document *doc = NULL

        handle_status("get_doc failed",
            u1db_get_doc(self._db, doc_id, &doc))
        if doc == NULL:
            return None
        pydoc = CDocument()
        pydoc._doc = doc
        return pydoc

    def get_docs(self, doc_ids, check_for_conflicts=True):
        cdef int n_doc_ids, conflicts
        cdef const_char_ptr *c_doc_ids

        _list_to_array(doc_ids, &c_doc_ids, &n_doc_ids)
        if check_for_conflicts:
            conflicts = 1
        else:
            conflicts = 0
        a_list = []
        handle_status("get_docs",
            u1db_get_docs(self._db, n_doc_ids, c_doc_ids,
                conflicts, <void*>a_list, _append_doc_to_list))
        free(<void*>c_doc_ids)
        return a_list

    def resolve_doc(self, CDocument doc, conflicted_doc_revs):
        cdef const_char_ptr *revs
        cdef int n_revs

        _list_to_array(conflicted_doc_revs, &revs, &n_revs)
        handle_status("resolve_doc",
            u1db_resolve_doc(self._db, doc._doc, n_revs, revs))
        free(<void*>revs)

    def get_doc_conflicts(self, doc_id):
        conflict_docs = []
        handle_status("get_doc_conflicts",
            u1db_get_doc_conflicts(self._db, doc_id, <void*>conflict_docs,
                _append_doc_to_list))
        return conflict_docs

    def delete_doc(self, CDocument doc):
        handle_status("Failed to delete %s" % (doc,),
            u1db_delete_doc(self._db, doc._doc))

    def whats_changed(self, generation=0):
        cdef int c_generation

        a_list = []
        c_generation = generation
        handle_status("whats_changed",
            u1db_whats_changed(self._db, &c_generation, <void*>a_list,
                               _append_doc_gen_to_list))
        return c_generation, a_list

    def _get_transaction_log(self):
        a_list = []
        handle_status("get_transaction_log",
            u1db__get_transaction_log(self._db, <void*>a_list,
                                      _append_doc_gen_to_list))
        return [doc_id for doc_id, gen in a_list]

    def _get_generation(self):
        cdef int generation
        handle_status("get_generation",
            u1db__get_generation(self._db, &generation))
        return generation

    def get_sync_generation(self, replica_uid):
        cdef int generation

        handle_status("get_sync_generation",
            u1db__get_sync_generation(self._db, replica_uid, &generation))
        return generation

    def set_sync_generation(self, replica_uid, generation):
        handle_status("set_sync_generation",
            u1db__set_sync_generation(self._db, replica_uid, generation))

    def _sync_exchange(self, docs_info, from_replica_uid, from_machine_rev,
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
        status = u1db__sync_exchange(self._db, from_replica_uid,
            from_machine_rev, last_known_rev,
            from_records, &new_records, &conflict_records)
        u1db__free_records(&from_records)
        if status != U1DB_OK:
            raise RuntimeError("Failed to _sync_exchange: %d" % (status,))

    def create_index(self, index_name, index_expression):
        cdef const_char_ptr *expressions
        cdef int n_expressions

        _list_to_array(index_expression, &expressions, &n_expressions)
        handle_status("create_index",
            u1db_create_index(self._db, index_name, n_expressions, expressions))
        free(<void*>expressions)

    def list_indexes(self):
        a_list = []
        handle_status("list_indexes",
            u1db_list_indexes(self._db, <void *>a_list,
                              _append_index_definition_to_list))
        return a_list

    def delete_index(self, index_name):
        handle_status("delete_index",
            u1db_delete_index(self._db, index_name))

    def get_from_index(self, index_name, key_values):
        cdef CQuery query
        cdef int status
        query = self._query_init(index_name)
        res = []
        status = U1DB_OK
        for entry in key_values:
            if len(entry) == 0:
                status = u1db_get_from_index(self._db, query._query,
                    <void*>res, _append_doc_to_list, 0, NULL)
            elif len(entry) == 1:
                status = u1db_get_from_index(self._db, query._query,
                    <void*>res, _append_doc_to_list, 1, <char*>entry[0])
            elif len(entry) == 2:
                status = u1db_get_from_index(self._db, query._query,
                    <void*>res, _append_doc_to_list, 2,
                    <char*>entry[0], <char*>entry[1])
            elif len(entry) == 3:
                status = u1db_get_from_index(self._db, query._query,
                    <void*>res, _append_doc_to_list, 3,
                    <char*>entry[0], <char*>entry[1], <char*>entry[2])
            elif len(entry) == 4:
                status = u1db_get_from_index(self._db, query._query,
                    <void*>res, _append_doc_to_list, 4,
                    <char*>entry[0], <char*>entry[1], <char*>entry[2],
                    <char*>entry[3])
            else:
                status = U1DB_NOT_IMPLEMENTED
            handle_status("get_from_index", status)
        return res

    def _query_init(self, index_name):
        cdef CQuery query
        query = CQuery()
        handle_status("query_init",
            u1db_query_init(self._db, index_name, &query._query))
        return query
    
    def get_sync_target(self):
        cdef CSyncTarget target
        target = CSyncTarget()
        target._db = self
        handle_status("get_sync_target",
            u1db__get_sync_target(target._db._db, &target._st))
        return target


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
        cdef int gen
        if self._clock == NULL:
            return None
        res = {}
        for i from 0 <= i < self._clock.num_items:
            gen = self._clock.items[i].generation
            res[self._clock.items[i].replica_uid] = gen
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

    def increment(self, replica_uid):
        cdef int status

        status = u1db__vectorclock_increment(self._clock, replica_uid)
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


def sync_db_to_target(db, target):
    """Sync the data between a CDatabase and a CSyncTarget"""
    cdef CDatabase cdb
    cdef CSyncTarget ctarget
    cdef int local_gen = 0

    cdb = db
    ctarget = target
    handle_status("sync_db_to_target",
        u1db__sync_db_to_target(cdb._db, ctarget._st, &local_gen))
    return local_gen


def create_http_sync_target(url):
    cdef CSyncTarget target

    target = CSyncTarget()
    handle_status("create_http_sync_target",
        u1db__create_http_sync_target(url, &target._st))
    return target


def _format_sync_url(target, source_replica_uid):
    cdef CSyncTarget st
    cdef char *sync_url = NULL
    cdef object res
    st = target
    handle_status("format_sync_url",
        u1db__format_sync_url(st._st, source_replica_uid, &sync_url))
    if sync_url == NULL:
        res = None
    else:
        res = sync_url
        free(sync_url)
    return res
