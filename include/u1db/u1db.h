/*
 * Copyright 2011-2012 Canonical Ltd.
 *
 * This file is part of u1db.
 *
 * u1db is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation.
 *
 * u1db is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with u1db.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _U1DB_H_
#define _U1DB_H_

typedef struct _u1database u1database;
// The document structure. Note that you must use u1db_make_doc to create
// these, as there are private attributes that are required. This is just the
// public interface
typedef struct _u1db_document
{
    char *doc_id;
    size_t doc_id_len;
    char *doc_rev;
    size_t doc_rev_len;
    char *content;
    size_t content_len;
    int has_conflicts;
} u1db_document;


#define U1DB_OK 0
#define U1DB_INVALID_PARAMETER -1
// put_doc() was called but the doc_rev stored in the database doesn't match
// the one supplied.
#define U1DB_REVISION_CONFLICT -2
#define U1DB_INVALID_DOC_ID -3
#define U1DB_DOCUMENT_ALREADY_DELETED -4
#define U1DB_DOCUMENT_DOES_NOT_EXIST -5
#define U1DB_NOMEM -6

/**
 * The basic constructor for a new connection.
 */
u1database *u1db_open(const char *fname);

/**
 * Close an existing connection, freeing memory, etc.
 * This is generally used as u1db_free(&db);
 * After freeing the memory, we will set the pointer to NULL.
 */
void u1db_free(u1database **db);

/**
 * Set the machine_id defined for this database.
 */
int u1db_set_machine_id(u1database *db, const char *machine_id);

/**
 * Get the machine_id defined for this database.
 */
int u1db_get_machine_id(u1database *db, char **machine_id);

/**
 * Create a new document.
 *
 * @param content: The JSON string representing the document. The content will
 *                 be copied and managed by the 'doc' parameter.
 * @param doc_id: A string identifying the document. If the value supplied is
 *      NULL, then a new doc_id will be generated.
 * @param doc_rev: The document revision. Callers are responsible for freeing
 *      the information.
 * @param doc: a u1db_document that will be allocated and needs to be freed
 *      with u1db_free_doc
 * @return a status code indicating success or failure.
 */
int u1db_create_doc(u1database *db, const char *content, const char *doc_id,
                    u1db_document **doc);

/**
 * Put new document content for the given document identifier.
 *
 * @param doc: A document that we want to update. The new content should have
 *             been set with u1db_doc_set_content. The document's revision
 *             should match what is currently in the database, and will be
 *             updated to point at the new revision.
 */
int u1db_put_doc(u1database *db, u1db_document *doc);

/**
 * Get the document defined by the given document id.
 *
 * @param doc_id: The document we are looking for
 * @param doc: (OUT) a document (or NULL) matching the request
 * @return status, will be U1DB_OK if there is no error, even if there is no
 *      document matching that doc_id.
 */
int u1db_get_doc(u1database *db, const char *doc_id, u1db_document **doc);

/**
 * Mark a document as deleted.
 *
 * @param doc (IN/OUT) The document we want to delete, the document must match
 *                the stored value, or the delete will fail. After being
 *                deleted, the doc_rev parameter will be updated to match the
 *                new value in the database.
 */
int u1db_delete_doc(u1database *db, u1db_document *doc);

/**
 * Get the document defined by the given document id.
 *
 * @param db_rev The global database revision to start at. You can pass '0' to
 *               get all changes in the database. The integer will be updated
 *               to point at the current db_rev.
 * @param cb     A callback function. This will be called passing in 'context',
 *               and a document identifier for each document that has been modified.
 *               The doc_id string is transient, so callers must copy it to
 *               their own memory if they want to keep it.
 *               If a document is changed more than once, it is currently
 *               undefined whether this will call cb() once per change, or just
 *               once per doc_id.
 * @param context Opaque context, passed back to the caller.
 */
int u1db_whats_changed(u1database *db, int *db_rev,
                       int (*cb)(void *, char *doc_id), void *context);


/**
 * Internal API, Get the global database rev.
 */
int u1db__get_db_rev(u1database *db, int *db_rev);

/**
 * Internal API, Allocate a new document id, for cases when callers do not
 * supply their own. Callers of this API are expected to free the result.
 */
char *u1db__allocate_doc_id(u1database *db);

/**
 * Internal api, close the underlying sql instance.
 */
int u1db__sql_close(u1database *db);

/**
 * Internal api, check to see if the underlying SQLite handle has been closed.
 */
int u1db__sql_is_open(u1database *db);

/**
 * Check if a doc_id is valid.
 *
 * Returns U1DB_OK if everything is ok, otherwise U1DB_INVALID_DOC_ID.
 */
int u1db__is_doc_id_valid(const char *doc_id);

/**
 * Internal api, run an SQL query directly.
 */
typedef struct _u1db_row {
    struct _u1db_row *next;
    int num_columns; 
    int *column_sizes;
    unsigned char **columns;
} u1db_row;

typedef struct _u1db_table {
    int status;
    u1db_row *first_row;
} u1db_table;

u1db_table *u1db__sql_run(u1database *db, const char *sql, size_t n);
void u1db__free_table(u1db_table **table);


/**
 * Internal sync api, get the stored information about another machine.
 */
int u1db__sync_get_machine_info(u1database *db, const char *other_machine_id,
                            int *other_db_rev, char **my_machine_id,
                            int *my_db_rev);

/**
 * Internal sync api, store information about another machine.
 */
int u1db__sync_record_machine_info(u1database *db, const char *machine_id,
                                   int db_rev);

typedef struct _u1db_record {
    struct _u1db_record *next;
    char *doc_id;
    char *doc_rev;
    char *doc;
} u1db_record;

/**
 * Internal sync api, exchange sync records.
 */
int u1db__sync_exchange(u1database *db, const char *from_machine_id,
                        int from_db_rev, int last_known_rev,
                        u1db_record *from_records, u1db_record **new_records,
                        u1db_record **conflict_records);

/**
 * Allocate a new u1db_record, and copy all records over.
 */
u1db_record *u1db__create_record(const char *doc_id, const char *doc_rev,
                                 const char *doc);

u1db_record *u1db__copy_record(u1db_record *src);

/**
 * Free a linked list of records. All linked records will be freed, including
 * all memory referenced from them.
 */
void u1db__free_records(u1db_record **record);

typedef struct _u1db_vectorclock_item {
    char *machine_id;
    int db_rev;
} u1db_vectorclock_item;

typedef struct _u1db_vectorclock {
    int num_items;
    u1db_vectorclock_item *items;
} u1db_vectorclock;

u1db_vectorclock *u1db__vectorclock_from_str(const char *s);

void u1db__free_vectorclock(u1db_vectorclock **clock);
int u1db__vectorclock_increment(u1db_vectorclock *clock,
                                const char *machine_id);

int u1db__vectorclock_maximize(u1db_vectorclock *clock,
                               u1db_vectorclock *other);
/**
 * Return a null-terminated string representation for this vector clock.
 * Callers must take care to free() the result.
 */
int u1db__vectorclock_as_str(u1db_vectorclock *clock, char **result);
int u1db__vectorclock_is_newer(u1db_vectorclock *maybe_newer,
                               u1db_vectorclock *older);


/**
 * Create a new u1db_document object. This should be freed 
 */
u1db_document *u1db_make_doc(const char *doc_id, const char *revision,
                             const char *content, int has_conflicts);
void u1db_free_doc(u1db_document **doc);

/**
 * Set the content for the document.
 *
 * This will copy the string, since the memory is managed by the doc object
 * itself.
 */
int u1db_doc_set_content(u1db_document *doc, const char *content,
                         int content_len);
#endif // _U1DB_H_
