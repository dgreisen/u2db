/*
 * Copyright 2011 Canonical Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3, as published
 * by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranties of
 * MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include "u1db.h"

struct _u1database
{
    sqlite3 *sql_handle;
    char *machine_id;
};

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
    " machine_id TEXT PRIMARY KEY,"
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
    "CREATE TABLE u1db_config (name TEXT, value TEXT)",
    "INSERT INTO u1db_config VALUES ('sql_schema', '0')",
};

static int
initialize(u1database *db)
{
    sqlite3_stmt *statement;
    int i, status, final_status;

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
    free((*db)->machine_id);
    u1db__sql_close(*db);
    free(*db);
    *db = NULL;
}

int
u1db_set_machine_id(u1database *db, const char *machine_id)
{
    sqlite3_stmt *statement;
    int status, final_status, num_bytes;
    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT INTO u1db_config VALUES (?, ?)", -1,
        &statement, NULL); 
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, "machine_id", -1, SQLITE_STATIC);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        return status;
    }
    status = sqlite3_bind_text(statement, 2, machine_id, -1, SQLITE_TRANSIENT);
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
    // If we got this far, then machine_id has been properly set. Copy it
    if (db->machine_id != NULL) {
        free(db->machine_id);
    }
    num_bytes = strlen(machine_id);
    db->machine_id = (char *)calloc(1, num_bytes + 1);
    memcpy(db->machine_id, machine_id, num_bytes + 1);
    return 0;
}

int
u1db_get_machine_id(u1database *db, char **machine_id)
{
    sqlite3_stmt *statement;
    int status, num_bytes;
    const unsigned char *text;
    if (db->machine_id != NULL) {
        *machine_id = db->machine_id;
        return SQLITE_OK;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT value FROM u1db_config WHERE name = 'machine_id'", -1,
        &statement, NULL);
    if(status != SQLITE_OK) {
        *machine_id = "Failed to prepare statement";
        return status;
    }
    status = sqlite3_step(statement);
    if(status != SQLITE_ROW) {
        // TODO: Check return for failures
        sqlite3_finalize(statement);
        if (status == SQLITE_DONE) {
            // No machine_id set yet
            *machine_id = NULL;
            return SQLITE_OK;
        }
        *machine_id = "Failed to step prepared statement";
        return status;
    }
    if(sqlite3_column_count(statement) != 1) {
        sqlite3_finalize(statement);
        *machine_id = "incorrect column count";
        return status;
    }
    text = sqlite3_column_text(statement, 0);
    num_bytes = sqlite3_column_bytes(statement, 0);
    db->machine_id = (char *)calloc(1, num_bytes + 1);
    memcpy(db->machine_id, text, num_bytes+1);
    *machine_id = db->machine_id;
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
        return SQLITE_NOMEM;
    }
    if (*row != NULL) {
        (*row)->next = new_row;
    }
    (*row) = new_row;
    new_row->next = NULL;
    new_row->num_columns = sqlite3_column_count(statement);

    new_row->column_sizes = (int*)calloc(new_row->num_columns, sizeof(int));
    if (new_row->column_sizes == NULL) {
        return SQLITE_NOMEM;
    }
    new_row->columns = (unsigned char**)calloc(new_row->num_columns,
                                               sizeof(unsigned char *));
    if (new_row->columns == NULL) {
        return SQLITE_NOMEM;
    }
    for (i = 0; i < new_row->num_columns; i++) {
        text = sqlite3_column_text(statement, i);
        // This size does not include the NULL terminator.
        num_bytes = sqlite3_column_bytes(statement, i);
        new_row->column_sizes[i] = num_bytes;
        new_row->columns[i] = (unsigned char*)calloc(num_bytes+1, 1);
        if (new_row->columns[i] == NULL) {
            return SQLITE_NOMEM;
        }
        memcpy(new_row->columns[i], text, num_bytes+1);
    }
    return SQLITE_OK;
}

int
u1db_create_doc(u1database *db, const char *doc, int n, char **doc_id,
                char **doc_rev)
{
    if (db == NULL || doc == NULL || doc_id == NULL || doc_rev == NULL) {
        // Bad parameter
        return -1;
    }
    if (*doc_id == NULL) {
        *doc_id = u1db__allocate_doc_id(db);
    }
    return u1db_put_doc(db, *doc_id, doc_rev, doc, n);
}


// Lookup the contents for doc_id. We return the statement object, since it
// defines the lifetimes of doc and doc_rev. Callers should then finalize
// statement when they are done with them. 
static int
lookup_doc(u1database *db, const char *doc_id,
           const unsigned char **doc_rev, const unsigned char **doc, int *n,
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
        *doc = NULL;
        *n = 0;
        status = SQLITE_OK;
    } else if (status == SQLITE_ROW) {
        *doc_rev = sqlite3_column_text(*statement, 0);
        *doc = sqlite3_column_text(*statement, 1);
        *n = sqlite3_column_bytes(*statement, 1);
        status = SQLITE_OK;
    } else { // Error
    }
    return status;
}

// Insert the document into the table, we've already done the safety checks
static int
write_doc(u1database *db, const char *doc_id, const char *doc_rev,
          const char *doc, int n, int is_update)
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
    status = sqlite3_bind_text(statement, 2, doc, n, SQLITE_TRANSIENT);
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
u1db_put_doc(u1database *db, const char *doc_id, char **doc_rev,
             const char *doc, int n)
{
    const unsigned char *old_doc, *old_doc_rev;
    int status;
    int old_doc_n;
    sqlite3_stmt *statement;

    if (db == NULL || doc == NULL || doc_rev == NULL) {
        // Bad parameter
        return -1;
    }
    if (doc_id == NULL) {
        return U1DB_INVALID_DOC_ID;
    }
    sqlite3_exec(db->sql_handle, "BEGIN", 0, 0, 0);
    old_doc = NULL;
    status = lookup_doc(db, doc_id, &old_doc_rev, &old_doc, &old_doc_n, &statement);
    if (status != SQLITE_OK) {
        sqlite3_finalize(statement);
        sqlite3_exec(db->sql_handle, "ROLLBACK", 0, 0, 0);
        return status;
    }
    if (*doc_rev == NULL) {
        if (old_doc_rev == NULL) {
            // We are creating a new document from scratch. No problem.
            status = 0;
        } else {
            // We were supplied a NULL doc rev, but the doc already exists
            status = U1DB_INVALID_DOC_REV;
        }
    } else {
        if (old_doc_rev == NULL) {
            // TODO: Handle this case, it is probably just
            //       U1DB_INVALID_DOC_REV, but we want a test case first.
            // User supplied an old_doc_rev, but there is no entry in the db.
            status = -12345;
        } else {
            if (strcmp(*doc_rev, (const char *)old_doc_rev) == 0) {
                // The supplied doc_rev exactly matches old_doc_rev, good enough
                status = 0;
            } else {
                // Invalid old rev, mark it as such
                status = U1DB_INVALID_DOC_REV;
            }
        }
    }
    sqlite3_finalize(statement);
    if (status == SQLITE_OK) {
        // We are ok to proceed, allocating a new document revision, and
        // storing the document
        *doc_rev = (char *)calloc(1, 128);
        memcpy(*doc_rev, "test:1", 6);
        status = write_doc(db, doc_id, *doc_rev, doc, n, (old_doc != NULL));
        if (status == SQLITE_OK) {
            sqlite3_exec(db->sql_handle, "COMMIT", 0, 0, 0);
        }
    }
    if (status != SQLITE_OK) {
        sqlite3_exec(db->sql_handle, "ROLLBACK", 0, 0, 0);
    }
    return status;
}

int
u1db_get_doc(u1database *db, const char *doc_id, char **doc_rev,
             char **doc, int *n, int *has_conflicts)
{
    int status = 0, local_n = 0;
    sqlite3_stmt *statement;
    const unsigned char *local_doc_rev, *local_doc;
    if (db == NULL || doc_id == NULL || doc_rev == NULL || doc == NULL || n == NULL
        || has_conflicts == NULL) {
        // Bad Parameters
        // TODO: we could handle has_conflicts == NULL meaning that the caller
        //       is ignoring conflicts, but we don't want to make it *too* easy
        //       to do so.
        return -1;
    }

    status = lookup_doc(db, doc_id, &local_doc_rev, &local_doc, &local_n,
                        &statement);
    if (status == SQLITE_OK) {
        if (local_doc_rev == NULL) {
            *doc_rev = NULL;
            *doc = NULL;
            *has_conflicts = 0;
            goto finish;
        }
        *doc = (char *)calloc(1, local_n + 1);
        if (*doc == NULL) {
            status = SQLITE_NOMEM;
            goto finish;
        }
        memcpy(*doc, local_doc, local_n);
        *n = local_n;
        local_n = strlen((const char*)local_doc_rev);
        *doc_rev = (char *)calloc(1, local_n+1);
        if (*doc_rev == NULL) {
            status = SQLITE_NOMEM;
            goto finish;
        }
        memcpy(*doc_rev, local_doc_rev, local_n);
        *has_conflicts = 0;
    } else {
        *doc_rev = NULL;
        *doc = NULL;
        *n = 0;
        *has_conflicts = 0;
    }
finish:
    sqlite3_finalize(statement);
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
u1db__get_db_rev(u1database *db)
{
    int status, rev_num;
    sqlite3_stmt *statement;
    if (db == NULL) {
        return -1;
    }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT max(db_rev) FROM transaction_log", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return -status;
    }
    status = sqlite3_step(statement);
    if (status != SQLITE_ROW) {
        sqlite3_finalize(statement);
        if (status == SQLITE_DONE) {
            return 0;
        }
        return -status;
    }
    if(sqlite3_column_count(statement) != 1) {
        sqlite3_finalize(statement);
        return -1;
    }
    rev_num = sqlite3_column_int(statement, 0);
    status = sqlite3_finalize(statement);
    if (status != SQLITE_OK) {
        return -status;
    }
    return status;
}

char *
u1db__allocate_doc_id(u1database *db)
{
    int db_rev;
    char *buf;
    db_rev = u1db__get_db_rev(db);
    if(db_rev < 0) {
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
