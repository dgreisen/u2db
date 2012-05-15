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

#include "u1db/compat.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>

#include "u1db/u1db_internal.h"
#include "u1db/u1db_vectorclock.h"

// "u1do"
#define U1DB_DOCUMENT_MAGIC 0x7531646f
typedef struct _u1db_document_internal
{
    u1db_document doc;
    int magic; // Used to ensure people are passing a real internal document
    struct _u1db_document_internal *next; // Used when we need a linked list
    int generation; // Part of the sync api
} u1db_document_internal;


static int increment_doc_rev(u1database *db, const char *cur_rev,
                             char **doc_rev);
static int
initialize(u1database *db)
{
    sqlite3_stmt *statement;
    int i, status, final_status;
    char default_replica_uid[33] = {'\0'};

    for(i = 0; i < u1db__schema_len; i++) {
        status = sqlite3_prepare_v2(db->sql_handle,
            u1db__schema[i], -1, &statement, NULL);
        if(status != SQLITE_OK) {
            // fprintf(stderr, "Could not compile the %d statement:\n%s\n",
            //         i, u1db__schema[i]);
            return status;
        }
        status = sqlite3_step(statement);
        final_status = sqlite3_finalize(statement);
        if(status != SQLITE_DONE) {
            // fprintf(stderr, "Failed to step %d:\n%s\n",
            //         i, u1db__schema[i]);
            return status;
        }
        if(final_status != SQLITE_OK) {
            return final_status;
        }
    }
    u1db__generate_hex_uuid(default_replica_uid);
    u1db_set_replica_uid(db, default_replica_uid);
    return SQLITE_OK;
}

u1database *
u1db_open(const char *fname)
{
    u1database *db = (u1database *)(calloc(1, sizeof(u1database)));
    int status;
    status = sqlite3_open(fname, &db->sql_handle);
    if(status != SQLITE_OK) {
        // What do we do here?
        free(db);
        return NULL;
    }
    initialize(db);
    return db;
}

int
u1db__sql_close(u1database *db)
{
    if (db->sql_handle != NULL) {
        // sqlite says closing a NULL handle is ok, but we don't want to trust that
        int status;
        status = sqlite3_close(db->sql_handle);
        db->sql_handle = NULL;
        return status;
    }
    return SQLITE_OK;
}

int
u1db__sql_is_open(u1database *db)
{
    if (db != NULL && db->sql_handle != NULL) {
        // The handle is still open
        return 1;
    }
    return 0;
}

void
u1db_free(u1database **db)
{
    if (db == NULL || *db == NULL) {
        return;
    }
    free((*db)->replica_uid);
    u1db__sql_close(*db);
    free(*db);
    *db = NULL;
}

int
u1db_set_replica_uid(u1database *db, const char *replica_uid)
{
    sqlite3_stmt *statement;
    int status, final_status, num_bytes;
    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT OR REPLACE INTO u1db_config VALUES ('replica_uid', ?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, replica_uid, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_step(statement);
    final_status = sqlite3_finalize(statement);
    if (status != SQLITE_DONE) {
        return status;
    }
    if (final_status != SQLITE_OK) {
        return final_status;
    }
    // If we got this far, then replica_uid has been properly set. Copy it
    if (db->replica_uid != NULL) {
        free(db->replica_uid);
    }
    num_bytes = strlen(replica_uid);
    db->replica_uid = (char *)calloc(1, num_bytes + 1);
    memcpy(db->replica_uid, replica_uid, num_bytes + 1);
    return 0;
}

int
u1db_get_replica_uid(u1database *db, const char **replica_uid)
{
    sqlite3_stmt *statement;
    int status, num_bytes;
    const unsigned char *text;
    if (db->replica_uid != NULL) {
        *replica_uid = db->replica_uid;
        return SQLITE_OK;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT value FROM u1db_config WHERE name = 'replica_uid'", -1,
        &statement, NULL);
    if(status != SQLITE_OK) {
        *replica_uid = "Failed to prepare statement";
        return status;
    }
    status = sqlite3_step(statement);
    if(status != SQLITE_ROW) {
        // TODO: Check return for failures
        sqlite3_finalize(statement);
        if (status == SQLITE_DONE) {
            // No replica_uid set yet
            *replica_uid = NULL;
            return SQLITE_OK;
        }
        *replica_uid = "Failed to step prepared statement";
        return status;
    }
    if(sqlite3_column_count(statement) != 1) {
        sqlite3_finalize(statement);
        *replica_uid = "incorrect column count";
        return status;
    }
    text = sqlite3_column_text(statement, 0);
    num_bytes = sqlite3_column_bytes(statement, 0);
    db->replica_uid = (char *)calloc(1, num_bytes + 1);
    memcpy(db->replica_uid, text, num_bytes+1);
    *replica_uid = db->replica_uid;
    return SQLITE_OK;
}

static int
handle_row(sqlite3_stmt *statement, u1db_row **row)
{
    // Note: If this was a performance critical function, we could do a
    // first-pass over the data and determine total size, and fit all that into
    // a single calloc call.
    u1db_row *new_row;
    const unsigned char *text;
    int num_bytes, i;

    new_row = (u1db_row *)calloc(1, sizeof(u1db_row));
    if (new_row == NULL) {
        return U1DB_NOMEM;
    }
    if (*row != NULL) {
        (*row)->next = new_row;
    }
    (*row) = new_row;
    new_row->next = NULL;
    new_row->num_columns = sqlite3_column_count(statement);

    new_row->column_sizes = (int*)calloc(new_row->num_columns, sizeof(int));
    if (new_row->column_sizes == NULL) {
        return U1DB_NOMEM;
    }
    new_row->columns = (unsigned char**)calloc(new_row->num_columns, sizeof(char *));
    if (new_row->columns == NULL) {
        return U1DB_NOMEM;
    }
    for (i = 0; i < new_row->num_columns; i++) {
        text = sqlite3_column_text(statement, i);
        // This size does not include the NULL terminator.
        num_bytes = sqlite3_column_bytes(statement, i);
        new_row->column_sizes[i] = num_bytes;
        new_row->columns[i] = (unsigned char*)calloc(num_bytes+1, 1);
        if (new_row->columns[i] == NULL) {
            return U1DB_NOMEM;
        }
        memcpy(new_row->columns[i], text, num_bytes+1);
    }
    return SQLITE_OK;
}

int
u1db_create_doc(u1database *db, const char *content, const char *doc_id,
                u1db_document **doc)
{
    char *local_doc_id = NULL;
    int status;

    if (db == NULL || content == NULL || doc == NULL || *doc != NULL) {
        // Bad parameter
        return U1DB_INVALID_PARAMETER;
    }
    if (doc_id == NULL) {
        local_doc_id = u1db__allocate_doc_id(db);
        if (local_doc_id == NULL) {
            status = U1DB_INVALID_DOC_ID;
            goto finish;
        }
        doc_id = local_doc_id;
    }
    *doc = u1db__allocate_document(doc_id, NULL, content, 0);
    if (*doc == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    status = u1db_put_doc(db, *doc);
finish:
    if (local_doc_id != NULL) {
        // u1db__allocate_document will copy the doc_id string, so we still
        // have to free our local content.
        free(local_doc_id);
    }
    if (status != U1DB_OK) {
        u1db_free_doc(doc);
    }
    return status;
}


/**
 * Lookup the contents for doc_id.
 *
 * The returned strings (doc_rev and content) have their memory managed by the
 * statement object. So only finalize the statement after you have finished
 * accessing them.
 */
static int
lookup_doc(u1database *db, const char *doc_id, const char **doc_rev,
           const char **content, int *content_len,
           sqlite3_stmt **statement)
{
    int status;

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_rev, content FROM document WHERE doc_id = ?", -1,
        statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(*statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_step(*statement);
    if (status == SQLITE_DONE) {
        *doc_rev = NULL;
        *content = NULL;
        *content_len = 0;
        status = SQLITE_OK;
    } else if (status == SQLITE_ROW) {
        *doc_rev = (const char *)sqlite3_column_text(*statement, 0);
        // fprintf(stderr, "column_type: %d\n", sqlite3_column_type(*statement, 1));
        if (sqlite3_column_type(*statement, 1) == SQLITE_NULL) {
            // fprintf(stderr, "column_type: NULL\n");
            *content = NULL;
            *content_len = 0;
        } else {
            *content = (const char *)sqlite3_column_text(*statement, 1);
            *content_len = sqlite3_column_bytes(*statement, 1);
        }
        status = SQLITE_OK;
    } else { // Error
    }
    return status;
}


static int
delete_old_fields(u1database *db, const char *doc_id)
{
    sqlite3_stmt *statement;
    int status;

    status = sqlite3_prepare_v2(db->sql_handle,
        "DELETE FROM document_fields WHERE doc_id = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


// Insert the document into the table, we've already done the safety checks
static int
write_doc(u1database *db, const char *doc_id, const char *doc_rev,
          const char *content, int content_len, int is_update)
{
    sqlite3_stmt *statement;
    int status;

    if (is_update) {
        status = sqlite3_prepare_v2(db->sql_handle,
            "UPDATE document SET doc_rev = ?, content = ? WHERE doc_id = ?", -1,
            &statement, NULL);
        if (status != SQLITE_OK) { goto finish; }
        status = delete_old_fields(db, doc_id);
    } else {
        status = sqlite3_prepare_v2(db->sql_handle,
            "INSERT INTO document (doc_rev, content, doc_id) VALUES (?, ?, ?)", -1,
            &statement, NULL);
    }
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_rev, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    if (content == NULL) {
        status = sqlite3_bind_null(statement, 2);
    } else {
        status = sqlite3_bind_text(statement, 2, content, content_len,
                                   SQLITE_TRANSIENT);
    }
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 3, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
    sqlite3_finalize(statement);
    if (status != SQLITE_OK) { goto finish; }
    status = u1db__update_indexes(db, doc_id, content);
    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT INTO transaction_log(doc_id) VALUES (?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


// Are there any conflicts for this doc?
static int
lookup_conflict(u1database *db, const char *doc_id, int *has_conflict)
{
    sqlite3_stmt *statement;
    int status;

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT 1 FROM conflicts WHERE doc_id = ? LIMIT 1", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_ROW) {
        // fprintf(stderr, "\nFound conflict for %s\n", doc_id);
        *has_conflict = 1;
        status = SQLITE_OK;
    } else if (status == SQLITE_DONE) {
        // fprintf(stderr, "\nNo conflict for %s\n", doc_id);
        status = SQLITE_OK;
        *has_conflict = 0;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


// Add a conflict for this doc
static int
write_conflict(u1database *db, const char *doc_id, const char *doc_rev,
               const char *content, int content_len)
{
    sqlite3_stmt *statement;
    int status;

    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT INTO conflicts VALUES (?, ?, ?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 2, doc_rev, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    if (content == NULL) {
        status = sqlite3_bind_null(statement, 3);
    } else {
        status = sqlite3_bind_text(statement, 3, content, content_len,
                                   SQLITE_TRANSIENT);
    }
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


int
u1db_put_doc(u1database *db, u1db_document *doc)
{
    const char *old_content = NULL, *old_doc_rev = NULL;
    int status;
    int old_content_len;
    int conflicted;
    sqlite3_stmt *statement = NULL;

    if (db == NULL || doc == NULL) {
        // Bad parameter
        return -1;
    }
    status = u1db__is_doc_id_valid(doc->doc_id);
    if (status != U1DB_OK) {
        return status;
    }
    status = sqlite3_exec(db->sql_handle, "BEGIN", NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = lookup_conflict(db, doc->doc_id, &conflicted);
    if (status != SQLITE_OK) { goto finish; }
    if (conflicted) {
        status = U1DB_CONFLICTED;
        goto finish;
    }
    old_content = NULL;
    status = lookup_doc(db, doc->doc_id, &old_doc_rev, &old_content,
                        &old_content_len, &statement);
    if (status != SQLITE_OK) { goto finish; }
    if (doc->doc_rev == NULL) {
        if (old_doc_rev == NULL) {
            // We are creating a new document from scratch. No problem.
            status = 0;
        } else {
            // We were supplied a NULL doc rev, but the doc already exists
            status = U1DB_REVISION_CONFLICT;
        }
    } else {
        if (old_doc_rev == NULL) {
            // TODO: Handle this case, it is probably just
            //       U1DB_REVISION_CONFLICT, but we want a test case first.
            // User supplied an old_doc_rev, but there is no entry in the db.
            status = U1DB_REVISION_CONFLICT;
        } else {
            if (strcmp(doc->doc_rev, (const char *)old_doc_rev) == 0) {
                // The supplied doc_rev exactly matches old_doc_rev, good
                // enough
                status = U1DB_OK;
            } else {
                // Invalid old rev, mark it as such
                status = U1DB_REVISION_CONFLICT;
            }
        }
    }
    if (status == U1DB_OK) {
        // We are ok to proceed, allocating a new document revision, and
        // storing the document
        char *new_rev;
        status = increment_doc_rev(db, old_doc_rev, &new_rev);
        if (status != U1DB_OK) { goto finish; }
        free(doc->doc_rev);
        doc->doc_rev = new_rev;
        doc->doc_rev_len = strlen(new_rev);
        status = write_doc(db, doc->doc_id, new_rev,
                           doc->content, doc->content_len,
                           (old_doc_rev != NULL));
    }
finish:
    sqlite3_finalize(statement);
    if (status == SQLITE_OK) {
        status = sqlite3_exec(db->sql_handle, "COMMIT", NULL, NULL, NULL);
    } else {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
    }
    return status;
}


static int
find_current_doc_for_conflict(u1database *db, const char *doc_id,
        void *context, int (*cb)(void *context, u1db_document *doc))
{
    // There is a row to handle, so we first must return the original doc.
    int status;
    sqlite3_stmt *statement;
    const char *doc_rev, *content;
    int content_len;
    u1db_document *cur_doc;
    // fprintf(stderr, "\nFound a row in conflicts for %s\n", doc_id);
    status = lookup_doc(db, doc_id, &doc_rev, &content, &content_len,
                              &statement);
    if (status == SQLITE_OK) {
        if (doc_rev == NULL) {
            // There is an entry in conflicts, but no entry in documents,
            // something is broken here, this is the closest error we have
            status = U1DB_DOCUMENT_DOES_NOT_EXIST;
            goto finish;
        }
        cur_doc = u1db__allocate_document(doc_id, doc_rev, content, 1);
        if (cur_doc == NULL) {
            status = U1DB_NOMEM;
        } else {
            cb(context, cur_doc);
        }
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


int
u1db_get_doc_conflicts(u1database *db, const char *doc_id, void *context,
                       u1db_doc_callback cb)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;
    u1db_document *cur_doc;
    const char *doc_rev, *content;

    if (db == NULL || doc_id == NULL || cb == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_rev, content FROM conflicts WHERE doc_id = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_ROW) {
        int local_status;
        local_status = find_current_doc_for_conflict(db, doc_id, context, cb);
        if (local_status != U1DB_OK) {
            status = local_status;
            goto finish;
        }
    }
    while (status == SQLITE_ROW) {
        doc_rev = (const char*)sqlite3_column_text(statement, 0);
        if (sqlite3_column_type(statement, 1) == SQLITE_NULL) {
            content = NULL;
        } else {
            content = (const char*)sqlite3_column_text(statement, 1);
        }
        cur_doc = u1db__allocate_document(doc_id, doc_rev, content, 0);
        if (cur_doc == NULL) {
            // fprintf(stderr, "Failed to allocate_document\n");
            status = U1DB_NOMEM;
        } else {
            // fprintf(stderr, "Invoking cb for %s, %s\n", doc_id, doc_rev);
            cb(context, cur_doc);
            status = sqlite3_step(statement);
        }
    }
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}

static int
delete_conflict(u1database *db, const char *doc_id, const char *doc_rev)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;
    status = sqlite3_prepare_v2(db->sql_handle,
        "DELETE FROM conflicts WHERE doc_id = ? AND doc_rev = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 2, doc_rev, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = U1DB_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}

// Iterate through the stored conflicts, and remove ones which the new revision
// supersedes. By induction, the stored_rev should not supersede the saved
// conflicts, and we wouldn't be here if the new rev superseded the existing
// rev.
static int
prune_conflicts(u1database *db, u1db_document *doc,
                u1db_vectorclock *new_vc)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_rev FROM conflicts WHERE doc_id = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, doc->doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        const char *conflict_rev;
        u1db_vectorclock *conflict_vc;

        conflict_rev = (const char*)sqlite3_column_text(statement, 0);
        conflict_vc = u1db__vectorclock_from_str(conflict_rev);
        if (conflict_vc == NULL) {
            status = U1DB_NOMEM;
        } else {
            if (u1db__vectorclock_is_newer(new_vc, conflict_vc)) {
                // Note: Testing so far shows that it is ok to delete a record
                //        from a table that we are currently selecting. If we
                //        find out differently, update this to create a list of
                //        things to delete, then iterate over deleting them.
                status = delete_conflict(db, doc->doc_id, conflict_rev);
            } else {
                // There is an existing conflict that we do *not* supersede,
                // make sure the document is marked conflicted
                doc->has_conflicts = 1;
            }
            u1db__free_vectorclock(&conflict_vc);
        }
        if (status != SQLITE_ROW) {
            break;
        }
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = U1DB_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


int
u1db_put_doc_if_newer(u1database *db, u1db_document *doc, int save_conflict,
                      const char *replica_uid, int replica_gen, int *state,
                      int *at_gen)
{
    const char *stored_content = NULL, *stored_doc_rev = NULL;
    int status = U1DB_INVALID_PARAMETER, store = 0;
    int stored_content_len;
    sqlite3_stmt *statement;

    if (db == NULL || doc == NULL || state == NULL || doc->doc_rev == NULL) {
        return U1DB_INVALID_PARAMETER;
    }

    status = u1db__is_doc_id_valid(doc->doc_id);
    if (status != U1DB_OK) {
        return status;
    }
    status = sqlite3_exec(db->sql_handle, "BEGIN", NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    stored_content = NULL;
    status = lookup_doc(db, doc->doc_id, &stored_doc_rev, &stored_content,
                        &stored_content_len, &statement);
    if (status != SQLITE_OK) {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_finalize(statement);
        return status;
    }
    if (stored_doc_rev == NULL) {
        status = U1DB_OK;
        *state = U1DB_INSERTED;
        store = 1;
    } else if (strcmp(doc->doc_rev, (const char *)stored_doc_rev) == 0) {
        status = U1DB_OK;
        *state = U1DB_CONVERGED;
        store = 0;
    } else {
        u1db_vectorclock *stored_vc = NULL, *new_vc = NULL;
        // TODO: u1db__vectorclock_from_str returns NULL if there is an error
        //       in the vector clock, or if we run out of memory... Probably
        //       shouldn't be U1DB_NOMEM
        stored_vc = u1db__vectorclock_from_str(stored_doc_rev);
        if (stored_vc == NULL) {
            status = U1DB_NOMEM;
            goto finish;
        }
        new_vc = u1db__vectorclock_from_str(doc->doc_rev);
        if (new_vc == NULL) {
            status = U1DB_NOMEM;
            u1db__free_vectorclock(&stored_vc);
            goto finish;
        }
        if (u1db__vectorclock_is_newer(new_vc, stored_vc)) {
            // Just take the newer version
            store = 1;
            *state = U1DB_INSERTED;
            status = prune_conflicts(db, doc, new_vc);
        } else if (u1db__vectorclock_is_newer(stored_vc, new_vc)) {
            // The existing version is newer than the one supplied
            store = 0;
            status = U1DB_OK;
            *state = U1DB_SUPERSEDED;
        } else {
            // TODO: Handle the case where the vc strings are not identical,
            //       but they are functionally equivalent.
            // Neither is strictly newer than the other, so we treat this as a
            // conflict
            status = prune_conflicts(db, doc, new_vc);
            if (status == U1DB_OK) {
                *state = U1DB_CONFLICTED;
                store = save_conflict;
                if (save_conflict) {
                    status = write_conflict(db, doc->doc_id, stored_doc_rev,
                                            stored_content, stored_content_len);
                    doc->has_conflicts = 1;
                }
            }
        }
        u1db__free_vectorclock(&stored_vc);
        u1db__free_vectorclock(&new_vc);
    }
    if (status == U1DB_OK && store) {
        status = write_doc(db, doc->doc_id, doc->doc_rev,
                           doc->content, doc->content_len,
                           (stored_doc_rev != NULL));
    }
    if (status == U1DB_OK && replica_uid != NULL) {
        status = u1db__set_sync_generation(db, replica_uid, replica_gen);
    }
    if (status == U1DB_OK && at_gen != NULL) {
        status = u1db__get_generation(db, at_gen);
    }
finish:
    sqlite3_finalize(statement);
    if (status == SQLITE_OK) {
        status = sqlite3_exec(db->sql_handle, "COMMIT", NULL, NULL, NULL);
    } else {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
    }
    return status;
}


// Go through all of the revs, and make sure new_vc supersedes all of them
static int
ensure_maximal_rev(u1database *db, int n_revs, const char **revs,
                   u1db_vectorclock *new_vc)
{
    int i;
    int status = U1DB_OK;
    u1db_vectorclock *superseded_vc;
    const char *replica_uid;

    for (i = 0; i < n_revs; ++i) {
        superseded_vc = u1db__vectorclock_from_str(revs[i]);
        if (superseded_vc == NULL) {
            status = U1DB_NOMEM;
            goto finish;
        }
        u1db__vectorclock_maximize(new_vc, superseded_vc);
        u1db__free_vectorclock(&superseded_vc);
    }
    status = u1db_get_replica_uid(db, &replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__vectorclock_increment(new_vc, replica_uid);
finish:
    return status;
}


int
u1db_resolve_doc(u1database *db, u1db_document *doc,
                 int n_revs, const char **revs)
{
    int i = 0;
    int status = U1DB_OK;
    const char *stored_content, *stored_doc_rev;
    char *new_doc_rev;
    int stored_content_len;
    u1db_vectorclock *new_vc = NULL;
    sqlite3_stmt *statement;
    int cur_in_superseded = 0;

    if (db == NULL || doc == NULL || revs == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (n_revs == 0) {
        // Is it invalid to call resolve with no revs to resolve?
        return U1DB_OK;
    }
    for (i = 0; i < n_revs; ++i) {
        if (revs[i] == NULL) {
            return U1DB_INVALID_PARAMETER;
        }
    }
    status = lookup_doc(db, doc->doc_id, &stored_doc_rev, &stored_content,
                        &stored_content_len, &statement);
    if (status != SQLITE_OK) { goto finish; }
    // Check to see if one of the resolved revs is the current one
    cur_in_superseded = 0;
    if (stored_doc_rev == NULL) {
        // This seems an odd state, but we'll deal for now, everything
        // supersedes NULL
        cur_in_superseded = 1;
    } else {
        for (i = 0; i < n_revs; ++i) {
            if (strcmp(stored_doc_rev, revs[i]) == 0) {
                cur_in_superseded = 1;
            }
        }
    }
    new_vc = u1db__vectorclock_from_str(stored_doc_rev);
    if (new_vc == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    status = ensure_maximal_rev(db, n_revs, revs, new_vc);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__vectorclock_as_str(new_vc, &new_doc_rev);
    if (status != U1DB_OK) { goto finish; }
    free(doc->doc_rev);
    doc->doc_rev = new_doc_rev;
    doc->doc_rev_len = strlen(new_doc_rev);
    if (cur_in_superseded) {
        status = write_doc(db, doc->doc_id, new_doc_rev, doc->content,
                doc->content_len, (stored_doc_rev != NULL));
    } else {
        // The current value is not listed as being superseded, so we just put
        // this rev as a conflict
        status = write_conflict(db, doc->doc_id, new_doc_rev, doc->content,
                                doc->content_len);
    }
    if (status != U1DB_OK) {
        goto finish;
    }
    for (i = 0; i < n_revs; ++i) {
        status = delete_conflict(db, doc->doc_id, revs[i]);
        if (status != SQLITE_OK) { goto finish; }
    }
    // After deleting the conflicts, see if any remain
    status = lookup_conflict(db, doc->doc_id, &(doc->has_conflicts));
finish:
    u1db__free_vectorclock(&new_vc);
    sqlite3_finalize(statement);
    return status;
}


int
u1db_get_doc(u1database *db, const char *doc_id, u1db_document **doc)
{
    int status = 0, content_len = 0;
    sqlite3_stmt *statement;
    const char *doc_rev, *content;
    if (db == NULL || doc_id == NULL || doc == NULL) {
        // Bad Parameters
        return U1DB_INVALID_PARAMETER;
    }

    status = lookup_doc(db, doc_id, &doc_rev, &content, &content_len,
                        &statement);
    if (status == SQLITE_OK) {
        if (doc_rev == NULL) {
            // No such document exists
            *doc = NULL;
            goto finish;
        }
        *doc = u1db__allocate_document(doc_id, (const char*)doc_rev,
                                       (const char*)content, 0);

        if (*doc != NULL) {
            status = lookup_conflict(db, (*doc)->doc_id,
                                     &((*doc)->has_conflicts));
        }
    } else {
        *doc = NULL;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}

int
u1db_get_docs(u1database *db, int n_doc_ids, const char **doc_ids,
              int check_for_conflicts, void *context, u1db_doc_callback cb)
{
    int status, i;
    sqlite3_stmt *statement;

    if (db == NULL || doc_ids == NULL || cb == NULL || n_doc_ids < 0) {
        return U1DB_INVALID_PARAMETER;
    }
    for (i = 0; i < n_doc_ids; ++i) {
        if (doc_ids[i] == NULL) {
            return U1DB_INVALID_PARAMETER;
        }
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_rev, content FROM document WHERE doc_id = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    for (i = 0; i < n_doc_ids; ++i) {
        status = sqlite3_bind_text(statement, 1, doc_ids[i], -1,
                                   SQLITE_TRANSIENT);
        if (status != SQLITE_OK) { goto finish; }
        status = sqlite3_step(statement);
        if (status == SQLITE_ROW) {
            // We have a document
            const char *revision;
            const char *content;
            u1db_document *doc;
            revision = (char *)sqlite3_column_text(statement, 0);
            content = (char *)sqlite3_column_text(statement, 1);
            doc = u1db__allocate_document(doc_ids[i], revision, content, 0);
            if (check_for_conflicts) {
                status = lookup_conflict(db, doc_ids[i], &(doc->has_conflicts));
            }
            cb(context, doc);
        } else if (status == SQLITE_DONE) {
            // This document doesn't exist
            // TODO: I believe the python implementation returns the Null
            //       document here (possibly just None)
        } else {
            // Unknown error
            goto finish;
        }
        status = sqlite3_step(statement);
        // Was there more than one matching entry?
        if (status != SQLITE_DONE) { goto finish; }
        status = sqlite3_reset(statement);
        if (status != SQLITE_OK) { goto finish; }
    }
finish:
    sqlite3_finalize(statement);
    return status;
}

// Take cur_rev, and update it to have a version incremented based on the
// database replica uid
static int
increment_doc_rev(u1database *db, const char *cur_rev, char **doc_rev)
{
    u1db_vectorclock *vc = NULL;
    const char *replica_uid;
    int status = U1DB_OK;

    vc = u1db__vectorclock_from_str(cur_rev);
    if (vc == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    status = u1db_get_replica_uid(db, &replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__vectorclock_increment(vc, replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__vectorclock_as_str(vc, doc_rev);
    if (status != U1DB_OK) { goto finish; }
finish:
    u1db__free_vectorclock(&vc);
    return status;
}

int
u1db_delete_doc(u1database *db, u1db_document *doc)
{
    int status, content_len;
    sqlite3_stmt *statement;
    const char *cur_doc_rev, *content;
    char *doc_rev = NULL;
    int conflicted;

    if (db == NULL || doc == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_exec(db->sql_handle, "BEGIN", NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = lookup_doc(db, doc->doc_id, &cur_doc_rev, &content, &content_len,
                        &statement);
    if (status != SQLITE_OK) { goto finish; }
    if (cur_doc_rev == NULL) {
        // Can't delete a doc that never existed
        status = U1DB_DOCUMENT_DOES_NOT_EXIST;
        goto finish;
    }
    if (content == NULL) {
        // Can't delete a doc is already deleted
        status = U1DB_DOCUMENT_ALREADY_DELETED;
        goto finish;
    }
    if (strcmp((const char *)cur_doc_rev, doc->doc_rev) != 0) {
        // The saved document revision doesn't match
        status = U1DB_REVISION_CONFLICT;
        goto finish;
    }
    status = lookup_conflict(db, doc->doc_id, &conflicted);
    if (status != SQLITE_OK) { goto finish; }
    if (doc->has_conflicts) {
        status = U1DB_CONFLICTED;
        goto finish;
    }
    // TODO: Handle deleting a document with conflicts
    status = increment_doc_rev(db, cur_doc_rev, &doc_rev);
    if (status != U1DB_OK) { goto finish; }
    status = write_doc(db, doc->doc_id, doc_rev, NULL, 0, 1);

finish:
    sqlite3_finalize(statement);
    if (status != SQLITE_OK) {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
    } else {
        status = sqlite3_exec(db->sql_handle, "COMMIT", NULL, NULL, NULL);
        free(doc->doc_rev);
        doc->doc_rev = doc_rev;
        doc->doc_rev_len = strlen(doc_rev);
        free(doc->content);
        doc->content = NULL;
        doc->content_len = 0;
    }
    return status;
}

int
u1db_whats_changed(u1database *db, int *gen, void *context,
                   int (*cb)(void *, const char *doc_id, int gen))
{
    int status;
    sqlite3_stmt *statement;
    if (db == NULL || gen == NULL || cb == NULL) {
        return -1; // Bad parameters
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT max(generation) as g, doc_id FROM transaction_log"
        " WHERE generation > ?"
        " GROUP BY doc_id ORDER BY g",
        -1, &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_int(statement, 1, *gen);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        int local_gen;
        char *doc_id;
        local_gen = sqlite3_column_int(statement, 0);
        if (local_gen > *gen) {
            *gen = local_gen;
        }
        doc_id = (char *)sqlite3_column_text(statement, 1);
        cb(context, doc_id, local_gen);
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
    sqlite3_finalize(statement);
    return status;
}


int
u1db__get_transaction_log(u1database *db, void *context,
                          u1db_doc_id_gen_callback cb)
{
    int status;
    sqlite3_stmt *statement;
    if (db == NULL || cb == NULL) {
        return -1; // Bad parameters
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT generation, doc_id FROM transaction_log"
        " ORDER BY generation",
        -1, &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        int local_gen;
        const char *doc_id;
        local_gen = sqlite3_column_int(statement, 0);
        doc_id = (const char *)sqlite3_column_text(statement, 1);
        cb(context, doc_id, local_gen);
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
    sqlite3_finalize(statement);
    return status;
}


int
u1db__get_generation(u1database *db, int *generation)
{
    int status;
    sqlite3_stmt *statement;
    if (db == NULL || generation == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT max(generation) FROM transaction_log", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        // No records, we are at rev 0
        status = SQLITE_OK;
        *generation = 0;
    } else if (status == SQLITE_ROW) {
        status = SQLITE_OK;
        *generation = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return status;
}

char *
u1db__allocate_doc_id(u1database *db)
{
    int status;
    char *buf;
    buf = (char *)calloc(35, 1);
    if (buf == NULL) {
        return NULL;
    }
    buf[0] = 'D';
    buf[1] = '-';
    status = u1db__generate_hex_uuid(&buf[2]);
    if (status != U1DB_OK) {
        free(buf);
        return NULL;
    }
    return buf;
}

u1db_table *
u1db__sql_run(u1database *db, const char *sql, size_t n)
{
    // TODO: This could be simplified *a lot* by using sqlite3_exec
    int status, do_continue;
    u1db_table *result = NULL;
    u1db_row *cur_row = NULL;
    sqlite3_stmt *statement;
    result = (u1db_table *)calloc(1, sizeof(u1db_table));
    if (result == NULL) {
        return NULL;
    }
    status = sqlite3_prepare_v2(db->sql_handle, sql, n, &statement, NULL);
    if (status != SQLITE_OK) {
        result->status = status;
        return result;
    }
    do_continue = 1;
    while(do_continue) {
        do_continue = 0;
        status = sqlite3_step(statement);
        switch(status) {
            case SQLITE_DONE:
                result->status = SQLITE_OK;
                break;
            case SQLITE_ROW:
                {
                    status = handle_row(statement, &cur_row);
                    if (result->first_row == NULL) {
                        result->first_row = cur_row;
                    }
                    if (status == SQLITE_OK) {
                        do_continue = 1;
                    }
                }
                break;
            default: // Assume it is an error
                result->status = status;
                break;
        }
    }
    sqlite3_finalize(statement);
    return result;
}

void
u1db__free_table(u1db_table **table)
{
    u1db_row *cur_row, *old_row;
    int i;
    if (table == NULL || (*table) == NULL) {
        return;
    }
    cur_row = (*table)->first_row;
    while (cur_row != NULL) {
        old_row = cur_row;
        cur_row = cur_row->next;
        free(old_row->column_sizes);
        old_row->column_sizes = NULL;
        for (i = 0; i < old_row->num_columns; i++) {
            free(old_row->columns[i]);
            old_row->columns[i] = NULL;
        }
        free(old_row->columns);
        old_row->columns = NULL;
        free(old_row);
    }
    (*table)->first_row = NULL;
    free(*table);
    *table = NULL;
}


int
u1db__get_sync_generation(u1database *db, const char *replica_uid,
                          int *generation)
{
    int status;
    sqlite3_stmt *statement;

    if (db == NULL || replica_uid == NULL || generation == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT known_generation FROM sync_log WHERE replica_uid = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, replica_uid, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
        *generation = 0;
    } else if (status == SQLITE_ROW) {
        *generation = sqlite3_column_int(statement, 0);
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


int
u1db__set_sync_generation(u1database *db, const char *replica_uid,
                          int generation)
{
    int status;
    sqlite3_stmt *statement;

    if (db == NULL || replica_uid == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    // TODO: Do we need BEGIN & COMMIT here? There is a single mutation, it
    //       doesn't seem like it needs anything but autocommit...
    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT OR REPLACE INTO sync_log VALUES (?, ?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, replica_uid, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_int(statement, 2, generation);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


int
u1db__sync_exchange(u1database *db, const char *from_replica_uid,
                    int from_db_rev, int last_known_rev,
                    u1db_record *from_records, u1db_record **new_records,
                    u1db_record **conflict_records)
{
    if (db == NULL || from_replica_uid == NULL || new_records == NULL
        || conflict_records == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    return U1DB_INVALID_PARAMETER;
}

u1db_record *
u1db__create_record(const char *doc_id, const char *doc_rev, const char *doc)
{
    // TODO: If we wanted, we could allocate one large block, and then point
    //       the arrays to the right locations therein.
    u1db_record *record;
    record = (u1db_record *)calloc(1, sizeof(u1db_record));
    if (record == NULL) {
        return NULL;
    }
    record->doc_id = strdup(doc_id);
    record->doc_rev = strdup(doc_rev);
    if (doc == NULL) {
        record->doc = NULL;
    } else {
        record->doc = strdup(doc);
    }
    return record;
}

u1db_record *
u1db__copy_record(u1db_record *src)
{
    if (src == NULL) {
        return NULL;
    }
    return u1db__create_record(src->doc_id, src->doc_rev, src->doc);
}

void u1db__free_records(u1db_record **record)
{
    u1db_record *cur, *last;
    if (record == NULL || *record == NULL) {
        return;
    }
    cur = *record;
    while (cur != NULL) {
        last = cur;
        cur = cur->next;
        free(last->doc_id);
        free(last->doc_rev);
        if (last->doc != NULL) {
            free(last->doc);
        }
        free(last);
    }
    *record = NULL;
}

static int
copy_str_and_len(char **dest, size_t *dest_len, const char *source)
{
    int source_len;
    if (dest == NULL || dest_len == NULL) {
        // Bad parameters
        return 0;
    }
    if (source == NULL) {
        *dest = NULL;
        *dest_len = 0;
        return 1;
    } else {
        source_len = strlen(source);
    }
    *dest = (char *) calloc(1, source_len + 1);
    if (*dest == NULL) {
        return 0;
    }
    memcpy(*dest, source, source_len);
    *dest_len = source_len;
    return 1;
}

u1db_document *
u1db__allocate_document(const char *doc_id, const char *revision,
                        const char *content, int has_conflicts)
{
    u1db_document *doc = (u1db_document *)(calloc(1, sizeof(u1db_document)));
    if (doc == NULL) { goto cleanup; }
    if (!copy_str_and_len(&doc->doc_id, &doc->doc_id_len, doc_id))
        goto cleanup;
    if (!copy_str_and_len(&doc->doc_rev, &doc->doc_rev_len, revision))
        goto cleanup;
    if (!copy_str_and_len(&doc->content, &doc->content_len, content))
        goto cleanup;
    doc->has_conflicts = has_conflicts;
    return doc;
cleanup:
    if (doc == NULL) {
        return NULL;
    }
    if (doc->doc_id != NULL) {
        free(doc->doc_id);
    }
    if (doc->doc_rev != NULL) {
        free(doc->doc_id);
    }
    if (doc->content != NULL) {
        free(doc->content);
    }
    free(doc);
    return NULL;
}

void
u1db_free_doc(u1db_document **doc)
{
    if (doc == NULL || *doc == NULL) {
        return;
    }
    if ((*doc)->doc_id != NULL) {
        free((*doc)->doc_id);
    }
    if ((*doc)->doc_rev != NULL) {
        free((*doc)->doc_rev);
    }
    if ((*doc)->content != NULL) {
        free((*doc)->content);
    }
    free(*doc);
    *doc = NULL;
}


int
u1db_doc_set_content(u1db_document *doc, const char *content)
{
    char *tmp;
    int content_len;
    if (doc == NULL || content == NULL) {
        // TODO: return an error code
        return 0;
    }
    // What to do about 0 length content? Is it even valid? Not all platforms
    // support malloc(0)
    content_len = strlen(content);
    tmp = (char*)calloc(1, content_len + 1);
    if (tmp == NULL) {
        // TODO: return ENOMEM
        return 0;
    }
    memcpy(tmp, content, content_len);
    free(doc->content);
    doc->content = tmp;
    doc->content_len = content_len;
    // TODO: Return success
    return 1;
}

int
u1db__is_doc_id_valid(const char *doc_id)
{
    int len, i;
    unsigned char c;
    if (doc_id == NULL) {
        return U1DB_INVALID_DOC_ID;
    }
    len = strlen(doc_id);
    if (len == 0) {
        return U1DB_INVALID_DOC_ID;
    }
    for (i = 0; i < len; ++i) {
        c = (unsigned char)(doc_id[i]);
        // doc_id cannot contain slashes or characters outside the ascii range
        if (c == '\\' || c == '/' || c < ' ' || c > '~') {
            return U1DB_INVALID_DOC_ID;
        }
    }

    return U1DB_OK;
}

int
u1db_create_index(u1database *db, const char *index_name, int n_expressions,
                  const char **expressions)
{
    int status = U1DB_OK, i = 0;
    sqlite3_stmt *statement;
    const char **unique_expressions;
    int n_unique;

    if (db == NULL || index_name == NULL || expressions == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    for (i = 0; i < n_expressions; ++i) {
        if (expressions[i] == NULL) {
            return U1DB_INVALID_PARAMETER;
        }
    }
    status = sqlite3_exec(db->sql_handle, "BEGIN", NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = u1db__find_unique_expressions(db, n_expressions, expressions,
            &n_unique, &unique_expressions);
    if (status != U1DB_OK) { goto finish; }
    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT INTO index_definitions VALUES (?, ?, ?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, index_name, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    for (i = 0; i < n_expressions; ++i) {
        status = sqlite3_bind_int(statement, 2, i);
        if (status != SQLITE_OK) { goto finish; }
        status = sqlite3_bind_text(statement, 3, expressions[i], -1,
                                   SQLITE_TRANSIENT);
        if (status != SQLITE_OK) { goto finish; }
        status = sqlite3_step(statement);
        if (status != SQLITE_DONE) { goto finish; }
        status = sqlite3_reset(statement);
        if (status != SQLITE_OK) { goto finish; }
    }
    status = u1db__index_all_docs(db, n_unique, unique_expressions);
finish:
    if (unique_expressions != NULL) {
        free((void*)unique_expressions);
    }
    sqlite3_finalize(statement);
    if (status == SQLITE_OK) {
        status = sqlite3_exec(db->sql_handle, "COMMIT", NULL, NULL, NULL);
    } else {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
    }
    return status;
}


int
u1db_delete_index(u1database *db, const char *index_name)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;

    if (db == NULL || index_name == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "DELETE FROM index_definitions WHERE name = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, index_name, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status != SQLITE_DONE) { goto finish; }
    status = SQLITE_OK;
finish:
    sqlite3_finalize(statement);
    return status;
}


static void
free_expressions(int n_expressions, char **expressions)
{
    int i;
    if (expressions == NULL) {
        return;
    }
    for (i = 0; i < n_expressions; ++i) {
        if (expressions[i] != NULL) {
            free(expressions[i]);
            expressions[i] = NULL;
        }
    }
    free(expressions);
}


int
u1db_list_indexes(u1database *db, void *context,
                  int (*cb)(void *context, const char *index_name,
                            int n_expressions, const char **expressions))
{
    int status = U1DB_OK, n_expressions = -1;
    int offset;
    char *last_index_name = NULL;
    const char *index_name, *expression;
    sqlite3_stmt *statement;
    char **expressions = NULL;

    if (db == NULL || cb == NULL) {
        return U1DB_INVALID_PARAMETER;
    }

    // We query by offset descending, so we know how many entries there will be
    // after we see the first one.
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT name, offset, field FROM index_definitions"
        " ORDER BY name, offset DESC",
        -1, &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        index_name = (const char *)sqlite3_column_text(statement, 0);
        if (index_name == NULL) {
            status = U1DB_INVALID_PARAMETER; // TODO: better error code
            goto finish;
        }
        offset = sqlite3_column_int(statement, 1);
        expression = (const char *)sqlite3_column_text(statement, 2);
        if (expression == NULL) {
            status = U1DB_INVALID_PARAMETER; // TODO: better error code
            goto finish;
        }
        if (last_index_name != NULL && expressions != NULL
            && strcmp(last_index_name, index_name) != 0)
        {
            // offset should be 0, we should be at the last item in the list
            cb(context, last_index_name, n_expressions,
                (const char**)expressions);
            free_expressions(n_expressions, expressions);
            expressions = NULL;
            free(last_index_name);
            last_index_name = NULL;
        }
        if (expressions == NULL) {
            n_expressions = offset + 1;
            expressions = (char **)calloc(n_expressions, sizeof(char*));
            last_index_name = strdup(index_name);
        }
        if (offset >= n_expressions) {
            status = U1DB_INVALID_PARAMETER; // TODO: better error code
            goto finish;
        }
        expressions[offset] = strdup(expression);
        status = sqlite3_step(statement);
    }
    if (last_index_name != NULL && expressions != NULL) {
        // offset should be 0, we should be at the last item in the list
        cb(context, last_index_name, n_expressions,
            (const char**)expressions);
    }
    if (status == SQLITE_DONE) {
        status = U1DB_OK;
    }
finish:
    sqlite3_finalize(statement);
    free_expressions(n_expressions, expressions);
    if (last_index_name != NULL) {
        free(last_index_name);
    }
    return status;
}
