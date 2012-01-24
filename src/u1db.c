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

struct _u1database
{
    sqlite3 *sql_handle;
    char *replica_uid;
};

// "u1do"
#define U1DB_DOCUMENT_MAGIC 0x7531646f
typedef struct _u1db_document_internal
{
    u1db_document doc;
    int magic; // Used to ensure people are passing a real internal document
    struct _u1db_document_internal *next; // Used when we need a linked list
    int generation; // Part of the sync api
} u1db_document_internal;


static const char *table_definitions[] = {
    "CREATE TABLE transaction_log ("
    " db_rev INTEGER PRIMARY KEY AUTOINCREMENT,"
    " doc_id TEXT)",
    "CREATE TABLE document ("
    " doc_id TEXT PRIMARY KEY,"
    " doc_rev TEXT,"
    " doc TEXT)",
    "CREATE TABLE document_fields ("
    " doc_id TEXT,"
    " field_name TEXT,"
    " value TEXT,"
    " CONSTRAINT document_fields_pkey"
    " PRIMARY KEY (doc_id, field_name))",
    "CREATE TABLE sync_log ("
    " replica_uid TEXT PRIMARY KEY,"
    " known_db_rev INTEGER)",
    "CREATE TABLE conflicts ("
    " doc_id TEXT,"
    " doc_rev TEXT,"
    " doc TEXT,"
    " CONSTRAINT conflicts_pkey PRIMARY KEY (doc_id, doc_rev))",
    "CREATE TABLE index_definitions ("
    " name TEXT,"
    " offset INT,"
    " field TEXT,"
    " CONSTRAINT index_definitions_pkey"
    " PRIMARY KEY (name, offset))",
    "CREATE TABLE u1db_config (name TEXT PRIMARY KEY, value TEXT)",
    "INSERT INTO u1db_config VALUES ('sql_schema', '0')",
};

static int
initialize(u1database *db)
{
    sqlite3_stmt *statement;
    int i, status, final_status;
    char default_replica_uid[33] = {'\0'};

    for(i = 0; i < sizeof(table_definitions)/sizeof(char*); i++) {
        status = sqlite3_prepare_v2(db->sql_handle,
            table_definitions[i], -1, &statement, NULL);
        if(status != SQLITE_OK) {
            return status;
        }
        status = sqlite3_step(statement);
        final_status = sqlite3_finalize(statement);
        if(status != SQLITE_DONE) {
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
u1db_get_replica_uid(u1database *db, char **replica_uid)
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
    new_row->columns = (unsigned char**)calloc(new_row->num_columns,
                                               sizeof(unsigned char *));
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
lookup_doc(u1database *db, const char *doc_id, const unsigned char **doc_rev,
           const unsigned char **content, int *content_len,
           sqlite3_stmt **statement)
{
    int status;

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_rev, doc FROM document WHERE doc_id = ?", -1,
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
        *doc_rev = sqlite3_column_text(*statement, 0);
        // fprintf(stderr, "column_type: %d\n", sqlite3_column_type(*statement, 1));
        if (sqlite3_column_type(*statement, 1) == SQLITE_NULL) {
            // fprintf(stderr, "column_type: NULL\n");
            *content = NULL;
            *content_len = 0;
        } else {
            *content = sqlite3_column_text(*statement, 1);
            *content_len = sqlite3_column_bytes(*statement, 1);
        }
        status = SQLITE_OK;
    } else { // Error
    }
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
            "UPDATE document SET doc_rev = ?, doc = ? WHERE doc_id = ?", -1,
            &statement, NULL); 
    } else {
        status = sqlite3_prepare_v2(db->sql_handle, 
            "INSERT INTO document (doc_rev, doc, doc_id) VALUES (?, ?, ?)", -1,
            &statement, NULL); 
    }
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_rev, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    if (content == NULL) {
        status = sqlite3_bind_null(statement, 2);
    } else {
        status = sqlite3_bind_text(statement, 2, content, content_len,
                                   SQLITE_TRANSIENT);
    }
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_bind_text(statement, 3, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
    sqlite3_finalize(statement);
    if (status != SQLITE_OK) {
        return status;
    }
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
    sqlite3_finalize(statement);
    return status;
}

int
u1db_put_doc(u1database *db, u1db_document *doc)
{
    const unsigned char *old_content = NULL, *old_doc_rev = NULL;
    int status;
    int old_content_len;
    sqlite3_stmt *statement;

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
    old_content = NULL;
    status = lookup_doc(db, doc->doc_id, &old_doc_rev, &old_content,
                        &old_content_len, &statement);
    if (status != SQLITE_OK) {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_finalize(statement);
        return status;
    }
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
        u1db_vectorclock *vc;
        char *replica_uid, *new_rev;

        vc = u1db__vectorclock_from_str((char*)old_doc_rev);
        if (vc == NULL) { goto finish; }
        status = u1db_get_replica_uid(db, &replica_uid);
        if (status != U1DB_OK) { goto finish; }
        status = u1db__vectorclock_increment(vc, replica_uid);
        if (status != U1DB_OK) { goto finish; }
        status = u1db__vectorclock_as_str(vc, &new_rev);
        if (status != U1DB_OK) { goto finish; }
        free(doc->doc_rev);
        doc->doc_rev = new_rev;
        doc->doc_rev_len = strlen(new_rev);
        status = write_doc(db, doc->doc_id, new_rev,
                           doc->content, doc->content_len,
                           (old_content != NULL));
        if (status == SQLITE_OK) {
            status = sqlite3_exec(db->sql_handle, "COMMIT", NULL, NULL, NULL);
        }
    }
finish:
    sqlite3_finalize(statement);
    if (status != SQLITE_OK) {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
    }
    return status;
}

int
u1db_get_doc(u1database *db, const char *doc_id, u1db_document **doc)
{
    int status = 0, content_len = 0;
    sqlite3_stmt *statement;
    const unsigned char *doc_rev, *content;
    if (db == NULL || doc_id == NULL || doc == NULL) {
        // Bad Parameters
        // TODO: we could handle has_conflicts == NULL meaning that the caller
        //       is ignoring conflicts, but we don't want to make it *too* easy
        //       to do so.
        // TODO: Figure out how to do return codes
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

    } else {
        // TODO: Figure out how to return the SQL error code
        *doc = NULL;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}

int
u1db_delete_doc(u1database *db, u1db_document *doc)
{
    int status, content_len;
    sqlite3_stmt *statement;
    const unsigned char *cur_doc_rev, *content;
    char *doc_rev;

    if (db == NULL || doc == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_exec(db->sql_handle, "BEGIN", NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = lookup_doc(db, doc->doc_id, &cur_doc_rev, &content, &content_len,
                        &statement);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        return status;
    }
    if (cur_doc_rev == NULL || content == NULL) {
        // Can't delete a doc that doesn't exist
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_finalize(statement);
        if (cur_doc_rev == NULL) {
            return U1DB_DOCUMENT_DOES_NOT_EXIST;
        } else {
            return U1DB_DOCUMENT_ALREADY_DELETED;
        }
    }
    if (strcmp((const char *)cur_doc_rev, doc->doc_rev) != 0) {
        // The saved document revision doesn't match
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_finalize(statement);
        return U1DB_REVISION_CONFLICT;
    }
    // TODO: Handle conflicts
    sqlite3_finalize(statement);

    // TODO: Implement VectorClockRev
    doc_rev = (char *)calloc(1, 128);
    memcpy(doc_rev, "test:2", 6);
    status = write_doc(db, doc->doc_id, doc_rev, NULL, 0, 1);
    if (status != SQLITE_OK) {
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
    } else {
        status = sqlite3_exec(db->sql_handle, "COMMIT", NULL, NULL, NULL);
        // free(doc->doc_rev);
        doc->doc_rev = doc_rev;
        doc->doc_rev_len = 6;
    }
    return status;
}

int
u1db_whats_changed(u1database *db, int *db_rev,
                   int (*cb)(void *, char *doc_id), void *context)
{
    int status;
    sqlite3_stmt *statement;
    if (db == NULL || db_rev == NULL || cb == NULL) {
        return -1; // Bad parameters
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT db_rev, doc_id FROM transaction_log WHERE db_rev > ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_int(statement, 1, *db_rev);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        int local_db_rev;
        char *doc_id;
        local_db_rev = sqlite3_column_int(statement, 0);
        if (local_db_rev > *db_rev) {
            *db_rev = local_db_rev;
        }
        doc_id = (char *)sqlite3_column_text(statement, 1);
        cb(context, doc_id);
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
    sqlite3_finalize(statement);
    return status;
}


int
u1db__get_db_rev(u1database *db, int *db_rev)
{
    int status;
    sqlite3_stmt *statement;
    if (db == NULL || db_rev == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT max(db_rev) FROM transaction_log", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        // No records, we are at rev 0
        status = SQLITE_OK;
        *db_rev = 0;
    } else if (status == SQLITE_ROW) {
        status = SQLITE_OK;
        *db_rev = sqlite3_column_int(statement, 0);
    }
    sqlite3_finalize(statement);
    return status;
}

char *
u1db__allocate_doc_id(u1database *db)
{
    int db_rev, status;
    char *buf;
    status = u1db__get_db_rev(db, &db_rev);
    if(status != U1DB_OK) {
        // There was an error.
        return NULL;
    }
    buf = (char *)calloc(1, 128);
    snprintf(buf, 128, "doc-%d", db_rev);
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
u1db__sync_get_machine_info(u1database *db, const char *other_replica_uid,
                            int *other_db_rev, char **my_replica_uid,
                            int *my_db_rev)
{
    int status;
    sqlite3_stmt *statement;

    if (db == NULL || other_replica_uid == NULL || other_db_rev == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db_get_replica_uid(db, my_replica_uid);
    if (status != U1DB_OK) {
        return status;
    }
    status = u1db__get_db_rev(db, my_db_rev);
    if (status != U1DB_OK) {
        return status;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT known_db_rev FROM sync_log WHERE replica_uid = ?", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, other_replica_uid, -1,
                               SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
        *other_db_rev = 0;
    } else if (status == SQLITE_ROW) {
        *other_db_rev = sqlite3_column_int(statement, 0);
        status = SQLITE_OK;
    }
    sqlite3_finalize(statement);
    return status;
}

int
u1db__sync_record_machine_info(u1database *db, const char *replica_uid,
                               int db_rev)
{
    int status;
    sqlite3_stmt *statement;
    if (db == NULL || replica_uid == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = sqlite3_exec(db->sql_handle, "BEGIN", NULL, NULL, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT OR REPLACE INTO sync_log VALUES (?, ?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, replica_uid, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        return status;
    }
    status = sqlite3_bind_int(statement, 2, db_rev);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        sqlite3_exec(db->sql_handle, "ROLLBACK", NULL, NULL, NULL);
        return status;
    }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
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
    if (doc_id == NULL) {
        return U1DB_INVALID_DOC_ID;
    }
    len = strlen(doc_id);
    if (len == 0) {
        return U1DB_INVALID_DOC_ID;
    }
    for (i = 0; i < len; ++i) {
        if (doc_id[i] == '\\' || doc_id[i] == '/') {
            return U1DB_INVALID_DOC_ID;
        }
    }
    return U1DB_OK;
}
