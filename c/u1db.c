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
