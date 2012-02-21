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
#include <json/json.h>


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


void
u1db_free_query(u1query **query)
{
    int i;
    u1query *q;
    if (query == NULL || *query == NULL) {
        return;
    }
    q = *query;
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
                    const char *val0, void *context, u1db_doc_callback cb)
{
    int status = U1DB_OK;
    u1query *query = NULL;

    if (db == NULL || index_name == NULL || val0 == NULL || cb == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db_query_init(db, index_name, &query);
    if (status != U1DB_OK) { goto finish; }
    status = u1db_get_from_index(db, query, context, cb, 1, val0);
finish:
    u1db_free_query(&query);
    return status;
}


int
u1db_get_from_index(u1database *db, u1query *query,
                    void *context, u1db_doc_callback cb,
                    int n_values, const char *val0, ...)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;
    char *doc_id = NULL;
    u1db_document *doc = NULL;
    char *query_str = NULL;
    int i;
    va_list argp;
    char *valN = NULL;

    if (db == NULL || query == NULL || cb == NULL
        || n_values < 1 || val0 == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db__format_query(query, &query_str);
    if (status != U1DB_OK) { goto finish; }
    status = sqlite3_prepare_v2(db->sql_handle, query_str, -1,
                                &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    // Bind all of the 'field_name' parameters.
    for (i = 0; i < query->num_fields; ++i) {
        // for some reason bind_text starts at 1
        status = sqlite3_bind_text(statement, (i*2) + 1, query->fields[i], -1,
                                   SQLITE_TRANSIENT);
        if (status != SQLITE_OK) { goto finish; }
    }
    // Bind all of the value parameters
    status = sqlite3_bind_text(statement, 2, val0, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    va_start(argp, val0);
    for (i = 1; i < n_values; ++i) {
        valN = va_arg(argp, char *);
        status = sqlite3_bind_text(statement, (i*2)+2, valN, -1,
                                   SQLITE_TRANSIENT);
        if (status != SQLITE_OK) { goto finish; }
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        doc_id = (char*)sqlite3_column_text(statement, 0);
        status = u1db_get_doc(db, doc_id, &doc);
        if (status != U1DB_OK) { goto finish; }
        cb(context, doc);
        status = sqlite3_step(statement);
    }
    if (status != SQLITE_DONE) { goto finish; }
finish:
    va_end(argp);
    if (query_str != NULL) {
        free(query_str);
    }
    return status;
}


static void
add_to_buf(char **buf, int *buf_size, const char *fmt, ...)
{
    int count;
    va_list argp;
    va_start(argp, fmt);
    count = vsnprintf(*buf, *buf_size, fmt, argp);
    va_end(argp);
    *buf += count;
    *buf_size -= count;
}


int
u1db__format_query(u1query *query, char **buf)
{
    int status = U1DB_OK;
    int buf_size, i;
    char *cur;

    if (query->num_fields == 0) {
        return U1DB_INVALID_PARAMETER;
    }
    // 81 for 1 doc, 166 for 2, 251 for 3
    buf_size = (1 + query->num_fields) * 100;
    // The first field is treated specially
    cur = (char*)calloc(buf_size, 1);
    *buf = cur;
    add_to_buf(&cur, &buf_size, "SELECT d0.doc_id FROM document_fields d0");
    for (i = 1; i < query->num_fields; ++i) {
        add_to_buf(&cur, &buf_size, ", document_fields d%d", i);
    }
    add_to_buf(&cur, &buf_size, " WHERE d0.field_name = ? AND d0.value = ?");
    for (i = 1; i < query->num_fields; ++i) {
        add_to_buf(&cur, &buf_size,
            " AND d0.doc_id = d%d.doc_id"
            " AND d%d.field_name = ?"
            " AND d%d.value = ?",
            i, i, i);
    }
    return status;
}

struct sqlcb_to_field_cb {
    void *user_context;
    int (*user_cb)(void *, const char*);
};

// Thunk from the SQL interface, to a nicer single value interface
static int
sqlite_cb_to_field_cb(void *context, int n_cols, char **cols, char **rows)
{
    struct sqlcb_to_field_cb *ctx;
    ctx = (struct sqlcb_to_field_cb*)context;
    if (n_cols != 1) {
        return 1; // Error
    }
    return ctx->user_cb(ctx->user_context, cols[0]);
}


// Iterate over the fields that are indexed, and invoke cb for each one
static int
iter_field_definitions(u1database *db, void *context,
                      int (*cb)(void *context, const char *expression))
{
    int status;
    struct sqlcb_to_field_cb ctx;

    ctx.user_context = context;
    ctx.user_cb = cb;
    status = sqlite3_exec(db->sql_handle,
        "SELECT field FROM index_definitions",
        sqlite_cb_to_field_cb, &ctx, NULL);
    return status;
}

struct evaluate_index_context {
    u1database *db;
    const char *doc_id;
    json_object *obj;
    const char *content;
};

static int
add_to_document_fields(u1database *db, const char *doc_id, 
                       const char *expression, const char *val)
{
    int status;
    sqlite3_stmt *statement;

    status = sqlite3_prepare_v2(db->sql_handle,
        "INSERT INTO document_fields (doc_id, field_name, value)"
        " VALUES (?, ?, ?)", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, doc_id, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 2, expression, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_bind_text(statement, 3, val, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}

static int
evaluate_index_and_insert_into_db(void *context, const char *expression)
{
    struct evaluate_index_context *ctx;
    json_object *val;
    const char *str_val;
    int status = U1DB_OK;

    ctx = (struct evaluate_index_context *)context;
    if (ctx->obj == NULL || !json_object_is_type(ctx->obj, json_type_object)) {
        return U1DB_INVALID_JSON;
    }
    val = json_object_object_get(ctx->obj, expression);
    if (val != NULL) {
        str_val = json_object_get_string(val);
        if (str_val != NULL) {
            status = add_to_document_fields(ctx->db, ctx->doc_id, expression,
                    str_val);
        }
        json_object_put(val);
    }
    return status;
}

// Is this expression field already in the indexed list?
// We make an assumption that the number of new expressions is always small
// relative to what is already indexed (which should be reasonably accurate). 
static int
is_present(u1database *db, const char *expression, int *present)
{
    sqlite3_stmt *statement;
    int status;

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT 1 FROM index_definitions WHERE field = ? LIMIT 1", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    status = sqlite3_bind_text(statement, 1, expression, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) { goto finish; }
    status = sqlite3_step(statement);
    if (status == SQLITE_DONE) {
        status = SQLITE_OK;
        *present = 0;
    } else if (status == SQLITE_ROW) {
        status = SQLITE_OK;
        *present = 1;
    }
finish:
    sqlite3_finalize(statement);
    return status;
}


int
u1db__find_unique_expressions(u1database *db,
                              int n_expressions, const char **expressions,
                              int *n_unique, const char ***unique_expressions)
{
    int i, status, present = 0;
    const char **tmp;

    tmp = (char **)calloc(n_expressions, sizeof(char*));
    if (tmp == NULL) {
        return U1DB_NOMEM;
    }
    status = U1DB_OK;
    *n_unique = 0;
    for (i = 0; i < n_expressions; ++i) {
        if (expressions[i] == NULL) {
            status = U1DB_INVALID_PARAMETER;
            goto finish;
        }
        status = is_present(db, expressions[i], &present);
        if (status != SQLITE_OK) { goto finish; }
        if (!present) {
            tmp[*n_unique] = expressions[i];
            (*n_unique)++;
        }
    }
finish:
    if (status == U1DB_OK) {
        *unique_expressions = tmp;
    } else {
        free(tmp);
    }
    return status;
}


int
u1db__update_indexes(u1database *db, const char *doc_id, const char *content)
{
    struct evaluate_index_context context;
    int status;

    if (content == NULL) {
        // No new fields to add to the database.
        return U1DB_OK;
    }
    context.db = db;
    context.doc_id = doc_id;
    context.content = content;
    context.obj = json_tokener_parse(content);
    if (context.obj == NULL
            || !json_object_is_type(context.obj, json_type_object))
    {
        return U1DB_INVALID_JSON;
    }
    status = iter_field_definitions(db, &context,
            evaluate_index_and_insert_into_db);
    json_object_put(context.obj);
    return status;
}


int
u1db__index_all_docs(u1database *db, int n_expressions,
                     const char **expressions)
{
    int status, i;
    sqlite3_stmt *statement;
    struct evaluate_index_context context;

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_id, content FROM document", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    context.db = db;
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        context.doc_id = (const char*)sqlite3_column_text(statement, 0);
        context.content = (const char*)sqlite3_column_text(statement, 1);
        context.obj = json_tokener_parse(context.content);
        if (context.obj == NULL
                || !json_object_is_type(context.obj, json_type_object))
        {
            // Invalid JSON in the database, for now we just continue?
            continue;
        }
        for (i = 0; i < n_expressions; ++i) {
            status = evaluate_index_and_insert_into_db(&context, expressions[i]);
            if (status != U1DB_OK) { goto finish; }
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
