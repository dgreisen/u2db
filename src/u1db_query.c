/*
 * Copyright 2012 Canonical Ltd.
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

#include "u1db/u1db_internal.h"
#include <sqlite3.h>
#include <stdarg.h>


static int
lookup_index_fields(u1database *db, u1query *query)
{
    int status, offset;
    char *field;
    sqlite3_stmt *statement = NULL;

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT offset, field FROM index_definitions"
        " WHERE name = ?"
        " ORDER BY offset DESC",
        -1, &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, query->index_name, -1,
                               SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        offset = sqlite3_column_int(statement, 0);
        field = (char*)sqlite3_column_text(statement, 1);
        if (query->fields == NULL) {
            query->num_fields = offset + 1;
            query->fields = (char**)calloc(query->num_fields, sizeof(char*));
            if (query->fields == NULL) {
                status = U1DB_NOMEM;
                goto finish;
            }
        }
        if (offset >= query->num_fields) {
            status = U1DB_INVALID_PARAMETER; // TODO: better error code
            goto finish;
        }
        query->fields[offset] = strdup(field);
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
u1db_query_init(u1database *db, const char *index_name, u1query **query)
{
    int status;
    if (db == NULL || index_name == NULL || query == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    *query = (u1query*)calloc(1, sizeof(u1query));
    if (*query == NULL) {
        return U1DB_NOMEM;
    }
    // Should we be copying this instead?
    (*query)->index_name = index_name;
    status = lookup_index_fields(db, *query);
    if (status != U1DB_OK) {
        u1db_free_query(query);
    }
    return status;
}


int
u1db_query_add_entry(u1query *query, const char *value, ...)
{
    int status = U1DB_OK, i;
    int buffer_size, len;
    char *val, *data;
    u1query_entry *entry;
    va_list argp;

    if (query == NULL || value == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    // We allocate a u1query_entry as a single block with self-referencing
    // pointers. The structure is:
    // entry_data, values pointers, string0, string1, ..., stringN
    // So first we compute how much to allocate by iterating over the data,
    // then we allocate, and fill in all the pointers.
    va_start(argp, value);
    // the first argument is 'value', the others are in '...'
    buffer_size = sizeof(u1query_entry) + (sizeof(char*)*query->num_fields);
    buffer_size += strlen(value) + 1;
    for (i = 1; i < query->num_fields; ++i) {
        val = va_arg(argp, char *);
        if (val == NULL) {
            status = U1DB_INVALID_PARAMETER;
            goto finish;
        }
        buffer_size += strlen(val) + 1;
    }
    data = (char*)calloc(buffer_size, 1);
    if (data == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    entry = (u1query_entry*)data;
    data += sizeof(u1query_entry);
    entry->values = (char**)data;
    data += sizeof(char*)*query->num_fields;
    va_end(argp);
    va_start(argp, value);
    entry->values[0] = data;
    len = strlen(value);
    memcpy(entry->values[0], value, len);
    data += (len+1);
    for (i = 1; i < query->num_fields; ++i) {
        entry->values[i] = data;
        val = va_arg(argp, char *);
        len = strlen(val);
        memcpy(entry->values[i], val, len);
        data += len + 1;
    }
    query->num_entries += 1;
    entry->next = NULL;
    if (query->head == NULL) {
        query->head = entry;
    } else {
        query->last->next = entry;
    }
    query->last = entry;
finish:
    va_end(argp);
    return status;
}


void
u1db_free_query(u1query **query)
{
    int i;
    u1query *q;
    if (query == NULL || *query == NULL) {
        return;
    }
    q = *query;
    if (q->head != NULL) {
        u1query_entry *cur, *next = NULL;
        for (cur = q->head; cur != NULL; cur = next) {
            next = cur->next;
            free(cur);
        }
        q->head = NULL;
        q->last = NULL;
    }
    if (q->fields != NULL) {
        for (i = 0; i < q->num_fields; ++i) {
            if (q->fields[i] != NULL) {
                free(q->fields[i]);
                q->fields[i] = NULL;
            }
        }
        free(q->fields);
        q->fields = NULL;
    }
    free(*query);
    *query = NULL;
}


int
u1db_simple_lookup1(u1database *db, const char *index_name,
                    const char *val1, void *context, u1db_doc_callback cb)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;
    u1query *query = NULL;
    char *doc_id = NULL;
    u1db_document *doc = NULL;

    if (db == NULL || index_name == NULL || val1 == NULL || cb == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db_query_init(db, index_name, &query);
    if (status != U1DB_OK) { goto finish; }
    status = lookup_index_fields(db, query);
    if (status != U1DB_OK) { goto finish; }
    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_id FROM document_fields d0"
        " WHERE d0.field_name = ? AND d0.value = ?",
        -1, &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 1, query->fields[0], -1,
                               SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 2, val1, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        doc_id = (char*)sqlite3_column_text(statement, 0);
        status = u1db_get_doc(db, doc_id, &doc);
        if (status != U1DB_OK) { goto finish; }
        cb(context, doc);
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    u1db_free_query(&query);
    return status;
}
