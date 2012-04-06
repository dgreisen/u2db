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
#include <string.h>
#include <ctype.h>
#include <json/json.h>

#define OPERATIONS 1
static const char *OPERATORS[] = {"lower"};

typedef struct string_list_item_
{
    char *data;
    struct string_list_item_ *next;
} string_list_item;

typedef struct string_list_
{
    string_list_item *head;
    string_list_item *tail;
} string_list;

typedef string_list *(*operation)(const string_list *);
string_list *op_lower(const string_list *value);

static operation operations[] = {op_lower};

static void
init_list(string_list *list)
{
    list->head = NULL;
    list->tail = NULL;
}

static int
append(string_list *list, char *data)
{
    string_list_item *new_item;
    printf("append\n");
    if ((new_item = (string_list_item *)malloc(sizeof(string_list_item)))
            == NULL)
        return -1;
    printf("data: %s\n", data);
    new_item->data = data;
    new_item->next = NULL;
    if (list->head == NULL)
    {
        list->head = new_item;
    }
    if (list->tail != NULL)
        list->tail->next = new_item;
    list->tail = new_item;
    return 0;
}

static void
destroy_list(string_list *list)
{
    printf("destroy_list\n");
    if (list == NULL)
        return;
    string_list_item *item, *previous = NULL;
    item = list->head;
    while (item != NULL)
    {
        printf("data: %s\n", item->data);
        previous = item;
        item = item->next;
        free(previous->data);
        free(previous);
    }
    free(list);
    printf("destroy_list done\n");
}

static string_list *
list_copy(const string_list *original)
{
    string_list *copy = NULL;
    string_list_item *item = NULL;
    if ((copy = (string_list *)malloc(sizeof(string_list))) == NULL)
        return NULL;
    init_list(copy);
    for (item = original->head; item != NULL; item = item->next)
        if (append(copy, strdup(item->data)) == -1)
        {
            destroy_list(copy);
            return NULL;
        }
    return copy;
}

string_list *
op_lower(const string_list *values)
{
    string_list *result = NULL;
    string_list_item *item = NULL;
    if ((result = (string_list *)malloc(sizeof(string_list))) == NULL)
        return NULL;
    init_list(result);
    char *new_value, *value = NULL;

    printf("op_lower\n");
    for (item = values->head; item != NULL; item = item->next)
    {
        value = item->data;
        int i = 0;
        new_value = (char *)malloc(strlen(value) + 1);
        if (new_value != NULL)
        {
            while (value[i] != '\0')
            {
                // TODO: unicode hahaha
                new_value[i] = tolower(value[i]);
                i++;
            }
            new_value[i] = '\0';
        }
        if (append(result, new_value) == -1)
        {
            destroy_list(result);
            return NULL;
        }
    }
    return result;
}

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
                    int n_values, ...)
{
    int status = U1DB_OK;
    sqlite3_stmt *statement;
    char *doc_id = NULL;
    char *query_str = NULL;
    int i, bind_arg;
    va_list argp;
    const char *valN = NULL;
    int wildcard[20] = {0};

    if (db == NULL || query == NULL || cb == NULL || n_values < 0)
    {
        return U1DB_INVALID_PARAMETER;
    }
    if (query->num_fields != n_values) {
        return U1DB_INVALID_VALUE_FOR_INDEX;
    }
    if (n_values > 20) {
        return U1DB_NOT_IMPLEMENTED;
    }
    va_start(argp, n_values);
    status = u1db__format_query(query->num_fields, argp, &query_str, wildcard);
    va_end(argp);
    if (status != U1DB_OK) { goto finish; }
    status = sqlite3_prepare_v2(db->sql_handle, query_str, -1,
                                &statement, NULL);
    if (status != SQLITE_OK) { goto finish; }
    // Bind all of the 'field_name' parameters. sqlite_bind starts at 1
    bind_arg = 1;
    va_start(argp, n_values);
    for (i = 0; i < query->num_fields; ++i) {
        status = sqlite3_bind_text(statement, bind_arg, query->fields[i], -1,
                                   SQLITE_TRANSIENT);
        bind_arg++;
        if (status != SQLITE_OK) { goto finish; }
        valN = va_arg(argp, char *);
        if (wildcard[i] == 0) {
            // Not a wildcard, so add the argument
            status = sqlite3_bind_text(statement, bind_arg, valN, -1,
                                       SQLITE_TRANSIENT);
            bind_arg++;
        } else if (wildcard[i] == 2) {
            status = sqlite3_bind_text(statement, bind_arg, valN, -1,
                                       SQLITE_TRANSIENT);
            bind_arg++;
        }
        if (status != SQLITE_OK) { goto finish; }
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        doc_id = (char*)sqlite3_column_text(statement, 0);
        // We use u1db_get_docs so we can pass check_for_conflicts=0, which is
        // currently expected by the test suite.
        status = u1db_get_docs(db, 1, (const char**)&doc_id, 0, context, cb);
        if (status != U1DB_OK) { goto finish; }
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = U1DB_OK;
    }
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
u1db__format_query(int n_fields, va_list argp, char **buf, int *wildcard)
{
    int status = U1DB_OK;
    int buf_size, i;
    char *cur;
    const char *val;
    int have_wildcard = 0;

    if (n_fields < 1) {
        return U1DB_INVALID_PARAMETER;
    }
    // 81 for 1 doc, 166 for 2, 251 for 3
    buf_size = (1 + n_fields) * 100;
    // The first field is treated specially
    cur = (char*)calloc(buf_size, 1);
    if (cur == NULL) {
        return U1DB_NOMEM;
    }
    *buf = cur;
    add_to_buf(&cur, &buf_size, "SELECT d0.doc_id FROM document_fields d0");
    for (i = 1; i < n_fields; ++i) {
        add_to_buf(&cur, &buf_size, ", document_fields d%d", i);
    }
    add_to_buf(&cur, &buf_size, " WHERE d0.field_name = ?");
    for (i = 0; i < n_fields; ++i) {
        if (i != 0) {
            add_to_buf(&cur, &buf_size,
                " AND d0.doc_id = d%d.doc_id"
                " AND d%d.field_name = ?",
                i, i);
        }
        val = va_arg(argp, char *);
        if (val == NULL) {
            status = U1DB_INVALID_VALUE_FOR_INDEX;
            goto finish;
        }
        if (val[0] == '*') {
            wildcard[i] = 1;
            have_wildcard = 1;
            add_to_buf(&cur, &buf_size, " AND d%d.value NOT NULL", i);
        } else if (val[0] != '\0' && val[strlen(val)-1] == '*') {
            // glob
            wildcard[i] = 2;
            if (have_wildcard) {
                //globs not allowed after another wildcard
                status = U1DB_INVALID_VALUE_FOR_INDEX;
                goto finish;
            }
            have_wildcard = 1;
            add_to_buf(&cur, &buf_size, " AND d%d.value GLOB ?", i);
        } else {
            wildcard[i] = 0;
            if (have_wildcard) {
                // Can't have a non-wildcard after a wildcard
                status = U1DB_INVALID_VALUE_FOR_INDEX;
                goto finish;
            }
            add_to_buf(&cur, &buf_size, " AND d%d.value = ?", i);
        }
    }
finish:
    if (status != U1DB_OK && *buf != NULL) {
        free(*buf);
        *buf = NULL;
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

string_list *
extract_field_values(const char *expression, json_object *obj)
{
    char *lparen, *rparen, *sub, *data = NULL;
    char *result, *result_ptr, *dot_chr = NULL;
    struct array_list *list_val;
    string_list *values;
    json_object *val = NULL;
    int path_size, i;
    printf("extract_field_values\n");
    printf("expression: %s\n", expression);
    lparen = strchr(expression, '(');
    printf("lparen: %s\n", lparen);
    if (lparen == NULL)
    {
        result = strdup(expression);
        printf("result: %s\n", result);
        result_ptr = result;
        val = obj;
        while (result_ptr != NULL && val != NULL) {
            dot_chr = strchr(result_ptr, '.');
            if (dot_chr != NULL) {
                *dot_chr = '\0';
                dot_chr++;
            }
            // TODO: json_object uses ref-counting. Do we need to be
            //       json_object_put to the previous val so it gets cleaned up
            //       appropriately?
            val = json_object_object_get(val, result_ptr);
            result_ptr = dot_chr;
        }
        if (result != NULL)
        {
            free(result);
        }
        if (val == NULL)
        {
            return NULL;
        }
        if ((values = (string_list *)malloc(sizeof(string_list))) == NULL)
            return NULL;
        init_list(values);
        printf("**here\n");
        if (json_object_is_type(val, json_type_string)) {
            printf("here\n");
            data = strdup(json_object_get_string(val));
            printf("string data: %s\n", data);
            if (append(values, data) == -1)
            {
                printf("oops destroy_list\n");
                destroy_list(values);
                return NULL;
            }
        } else if (json_object_is_type(val, json_type_array)) {
            printf("here\n");
            list_val = json_object_get_array(val);
            for (i = 0; i < list_val->length; i++)
            {
                data = strdup(json_object_get_string(
                            array_list_get_idx(list_val, i)));
                printf("list data: %s\n", data);
                if (append(values, data) == -1)
                {
                    printf("oops destroy_list\n");
                    destroy_list(values);
                    return NULL;
                }
            }
        } else {
            printf("wtf\n");
        }
        json_object_put(val);
        return values;
    }
    printf("recursing\n");
    rparen = strrchr(expression, ')');
    if (rparen == NULL)
    {
        return NULL;
    }
    printf("rparen %s\n", rparen);
    path_size = ((rparen - 1) - (lparen + 1)) + 1;
    sub = (char *)malloc(path_size);
    if (sub != NULL)
    {
        strncpy(sub, lparen + 1, path_size);
        sub[path_size] = '\0';
        printf("sub %s\n", sub);
        values = extract_field_values(sub, obj);
        free(sub);
    }
    return values;
}

string_list *
apply_operations(const char *expression, const string_list *values)
{
    operation op = NULL;
    char *lparen, *op_name = NULL;
    int i, op_size;
    string_list *result, *tmp_values = NULL;
    printf("apply_operations\n");
    lparen = strchr(expression, '(');
    if (lparen == NULL)
    {
        result = list_copy(values);
        return result;
    }
    op_size = ((lparen - 1) - expression) + 1;
    op_name = (char *)malloc(op_size);
    if (op_name != NULL)
    {
        strncpy(op_name, expression, op_size);
        op_name[op_size] = '\0';
        for (i = 0; i < OPERATIONS; i++)
        {
            if (strcmp(OPERATORS[i], op_name) == 0)
            {
                op = operations[i];
                break;
            }
        }
        if (op == NULL)
        {
            // TODO: signal unknown operation
            goto finish;
        }
        tmp_values = apply_operations(lparen + 1, values);
        result = op(tmp_values);
    }
finish:
    printf("finally destroy_list\n");
    destroy_list(tmp_values);
    if (op != NULL)
    {
        free(op_name);
    }
    return result;
}

static int
evaluate_index_and_insert_into_db(void *context, const char *expression)
{
    struct evaluate_index_context *ctx;
    string_list *tmp_values, *values = NULL;
    string_list_item *item;
    int status = U1DB_OK;
    printf("evaluate_index_context\n");

    ctx = (struct evaluate_index_context *)context;
    if (ctx->obj == NULL || !json_object_is_type(ctx->obj, json_type_object)) {
        return U1DB_INVALID_JSON;
    }
    if ((tmp_values = extract_field_values(expression, ctx->obj)) == NULL)
    {
        goto finish;
    }
    if ((values = apply_operations(expression, tmp_values)) == NULL)
    {
        status = U1DB_NOMEM;
        goto finish;
    }
    for (item = values->head; item != NULL; item = item->next)
    {
        printf("data: %s\n", item->data);
        status = add_to_document_fields(
                ctx->db, ctx->doc_id, expression, item->data);
        if (status != U1DB_OK)
            goto finish;
    }
    printf("AOK\n");
finish:
    printf("tmp_values destroy_list\n");
    destroy_list(tmp_values);
    printf("values destroy_list\n");
    destroy_list(values);
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

    tmp = (const char **)calloc(n_expressions, sizeof(char*));
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
        free((void*)tmp);
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
