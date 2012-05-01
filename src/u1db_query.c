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

#define OPERATIONS 3
#ifndef max
    #define max(a, b) (((a) > (b)) ? (a) : (b))
#endif
static const char *OPERATORS[OPERATIONS] = {"lower", "number", "split_words"};

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

typedef struct transformation_
{
    void *op;
    struct transformation_ *next;
    string_list *args;
} transformation;

typedef int(*operation)(string_list *, const string_list *);
typedef int(*args_operation)(string_list *, const string_list *,
            const string_list *);
typedef int(*extract_operation)(string_list *, json_object *,
            const string_list *);
static int op_lower(string_list *result, const string_list *value);
static int op_number(string_list *result, const string_list *value,
                     const string_list *args);
static int op_split_words(string_list *result, const string_list *value);

static void *operations[OPERATIONS] = {
    op_lower, op_number, op_split_words};


static int
init_list(string_list **list)
{
    if ((*list = (string_list *)malloc(sizeof(string_list))) == NULL)
        return U1DB_NOMEM;
    (*list)->head = NULL;
    (*list)->tail = NULL;
    return U1DB_OK;
}

static int
append(string_list *list, const char *data)
{
    string_list_item *new_item = NULL;
    if ((new_item = (string_list_item *)malloc(sizeof(string_list_item)))
            == NULL)
        return U1DB_NOMEM;
    new_item->data = strdup(data);
    new_item->next = NULL;
    if (list->head == NULL)
    {
        list->head = new_item;
    }
    if (list->tail != NULL)
    {
        list->tail->next = new_item;
    }
    list->tail = new_item;
    return U1DB_OK;
}

static void
destroy_list(string_list *list)
{
    string_list_item *item, *previous = NULL;
    if (list == NULL)
        return;
    item = list->head;
    while (item != NULL)
    {
        previous = item;
        item = item->next;
        free(previous->data);
        free(previous);
    }
    list->head = NULL;
    list->tail = NULL;
    free(list);
    list = NULL;
}

static int
init_transformation(transformation **tr)
{
    int status = U1DB_OK;
    if ((*tr = (transformation *)malloc(sizeof(transformation))) == NULL)
        return U1DB_NOMEM;
    (*tr)->op = NULL;
    (*tr)->next = NULL;
    status = init_list(&((*tr)->args));
    if (status != U1DB_OK)
        return status;
    return status;
}

static void
destroy_transformation(transformation *tr)
{
    if (tr->next != NULL)
        destroy_transformation(tr->next);
    destroy_list(tr->args);
    free(tr);
}

static int
apply_transformation(transformation *tr, json_object *obj, string_list *result)
{
    int status = U1DB_OK;
    string_list *tmp_values = NULL;
    if (tr->next != NULL)
    {
        init_list(&tmp_values);
        status = apply_transformation(tr->next, obj, tmp_values);
        if (status != U1DB_OK)
            goto finish;
        if (tr->args->head != NULL)
        {
            status = ((args_operation)tr->op)(result, tmp_values, tr->args);
        } else {
            status = ((operation)tr->op)(result, tmp_values);
        }
    } else {
        status = ((extract_operation)tr->op)(result, obj, tr->args);
        goto finish;
    }
finish:
    destroy_list(tmp_values);
    return status;
}

static int
split(string_list *result, char *string, char splitter)
{
    int status = U1DB_OK;
    char *result_ptr, *split_point;
    result_ptr = string;
    while (result_ptr != NULL) {
        split_point = strchr(result_ptr, splitter);
        if (split_point != NULL) {
            *split_point = '\0';
            split_point++;
        }
        status = append(result, result_ptr);
        if (status != U1DB_OK)
            return status;
        result_ptr = split_point;
    }
    return status;
}

static int
list_index(string_list *list, char *data)
{
    int i = 0;
    string_list_item *item = NULL;
    for (item = list->head; item != NULL; item = item->next)
    {
        if (strcmp(item->data, data) == 0)
        {
            return i;
        }
        i++;
    }
    return -1;
}

static int
is_word_char(char c)
{
    if (isalnum(c))
    {
        return 0;
    }
    if (c == '.')
        return 0;
    if (c == '_')
        return 0;
    return -1;
}

static int
op_lower(string_list *result, const string_list *values)
{
    string_list_item *item = NULL;
    char *new_value, *value = NULL;
    int i;
    int status = U1DB_OK;

    for (item = values->head; item != NULL; item = item->next)
    {
        value = item->data;
        i = 0;
        new_value = (char *)calloc(strlen(value) + 1, 1);
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
        if ((status = append(result, new_value)) != U1DB_OK)
        {
            free(new_value);
            return status;
        }
        free(new_value);
    }
    return status;
}

static int
op_number(string_list *result, const string_list *values,
          const string_list *args)
{
    string_list_item *item = NULL;
    char *p, *new_value, *value, *number = NULL;
    int count, zeroes, value_size, isnumber;
    int status = U1DB_OK;

    number = args->head->data;
    for (p = number; *p; p++) {
        if (isdigit(*p) == 0) {
            status = U1DB_INVALID_VALUE_FOR_INDEX;
            goto finish;
        }
    }
    zeroes = atoi(number);

    for (item = values->head; item != NULL; item = item->next)
    {
        value = item->data;
        isnumber = 1;
        for (p = value; *p; p++) {
            if (isdigit(*p) == 0) {
                isnumber = 0;
                break;
            }
        }
        if (isnumber == 0) {
            continue;
        }
        value_size = max(strlen(value), zeroes) + 1;
        new_value = (char *)calloc(value_size, 1);
        if (new_value == NULL)
        {
            status = U1DB_NOMEM;
            goto finish;
        }
        count = snprintf(new_value, value_size, "%0*d", zeroes, atoi(value));
        if (count != (value_size - 1)) {
            // Most likely encoding issues.
            status = U1DB_INVALID_PARAMETER;
            goto finish;
        }
        if ((status = append(result, new_value)) != U1DB_OK)
        {
            free(new_value);
            goto finish;
        }
        free(new_value);
    }
finish:
    return status;
}

static int
op_split_words(string_list *result, const string_list *values)
{
    string_list_item *item = NULL;
    char *intermediate, *intermediate_ptr = NULL;
    char *space_chr = NULL;
    int status = U1DB_OK;
    for (item = values->head; item != NULL; item = item->next)
    {
        intermediate = strdup(item->data);
        intermediate_ptr = intermediate;
        while (intermediate_ptr != NULL) {
            space_chr = strchr(intermediate_ptr, ' ');
            if (space_chr != NULL) {
                *space_chr = '\0';
                space_chr++;
            }
            if (list_index(result, intermediate_ptr) == -1)
            {
                if ((status = append(result, intermediate_ptr)) != U1DB_OK)
                {
                    return status;
                }
            }
            intermediate_ptr = space_chr;
        }
        free(intermediate);
    }
    return status;
}

static int
lookup_index_fields(u1database *db, u1query *query)
{
    int status, offset;
    char *field = NULL;
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
    u1query *q = NULL;
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
    sqlite3_stmt *statement = NULL;
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
    sqlite3_finalize(statement);
    if (query_str != NULL) {
        free(query_str);
    }
    return status;
}

int
u1db_get_index_keys(u1database *db, char *index_name,
                    void *context, u1db_key_callback cb)
{
    int status = U1DB_OK;
    char *key = NULL;
    sqlite3_stmt *statement;
    status = sqlite3_prepare_v2(
        db->sql_handle,
        "SELECT document_fields.value FROM "
        "index_definitions INNER JOIN document_fields ON "
        "index_definitions.field = document_fields.field_name WHERE "
        "index_definitions.name = ? GROUP BY document_fields.value;",
        -1, &statement, NULL);
    if (status != SQLITE_OK) {
        goto finish;
    }
    status = sqlite3_bind_text(
        statement, 1, index_name, -1, SQLITE_TRANSIENT);
    if (status != SQLITE_OK) {
        goto finish;
    }
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        key = (char*)sqlite3_column_text(statement, 0);
        if ((status = cb(context, key)) != U1DB_OK)
            goto finish;
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = U1DB_OK;
    }
finish:
    sqlite3_finalize(statement);
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
    char *cur = NULL;
    const char *val = NULL;
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
    struct sqlcb_to_field_cb *ctx = NULL;
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
    sqlite3_stmt *statement = NULL;

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
extract_field_values(string_list *values, json_object *obj,
                     const string_list *field_path)
{
    string_list_item *item = NULL;
    char string_value[21];
    struct array_list *list_val = NULL;
    json_object *val = NULL;
    int i, integer_value;
    int status = U1DB_OK;
    val = obj;
    if (val == NULL)
        goto finish;
    for (item = field_path->head; item != NULL; item = item->next)
    {
        val = json_object_object_get(val, item->data);
        if (val == NULL)
            goto finish;
    }
    if (json_object_is_type(val, json_type_string)) {
        if ((status = append(values, json_object_get_string(val))) != U1DB_OK)
            goto finish;
    } else if (json_object_is_type(val, json_type_int)) {
        integer_value = json_object_get_int(val);
        snprintf(string_value, 21, "%d", integer_value);
        if (status != U1DB_OK)
            goto finish;
        if ((status = append(values, string_value)) != U1DB_OK)
            goto finish;
    } else if (json_object_is_type(val, json_type_array)) {
        list_val = json_object_get_array(val);
        for (i = 0; i < list_val->length; i++)
        {
            if ((status = append(values, json_object_get_string(
                                array_list_get_idx(
                                    list_val, i)))) != U1DB_OK)
                goto finish;
        }
    }
finish:
    return status;
}

static int
parse(const char *field, transformation *result)
{
    transformation *inner = NULL;
    char *new_field, *new_ptr, *argptr, *argend, *word, *first_comma = NULL;
    int status = U1DB_OK;
    int i, size;
    char *field_copy, *end = NULL;
    field_copy = strdup(field);
    end = field_copy;
    while (is_word_char(*end) == 0)
    {
        end++;
    }
    if (*end == '\0')
    {
        word = strdup(field_copy);
        if (word == NULL)
        {
            status = U1DB_NOMEM;
            goto finish;
        }
    }
    else {
        // TODO: unicode fieldnames ?
        size = (end - field_copy);
        word = (char *)calloc(size + 1, 1);
        strncpy(word, field_copy, size);
    }
    new_field = strdup(end);
    new_ptr = new_field;
    if (status != U1DB_OK)
        goto finish;
    if (*new_ptr == '(')
    {
        if (new_field[strlen(new_field) - 1] != ')')
        {
            status = U1DB_INVALID_TRANSFORMATION_FUNCTION;
            goto finish;
        }
        // step into parens
        new_ptr++;
        new_field[strlen(new_field) - 1] = '\0';
        for (i = 0; i < OPERATIONS; i++)
        {
            if (strcmp(OPERATORS[i], word) == 0)
            {
                result->op = operations[i];
                break;
            }
        }
        if (result == NULL)
        {
            status = U1DB_UNKNOWN_OPERATION;
            goto finish;
        }
        first_comma = strchr(new_field, ',');
        if (first_comma != NULL)
        {
            argptr = first_comma + 1;
            argend = argptr;
            *first_comma = '\0';
            while (*argend != '\0')
            {
                if (*argend == ',')
                {
                    *argend = '\0';
                    while (*argptr == ' ')
                        argptr++;
                    status = append(result->args, argptr);
                    if (status != U1DB_OK)
                        goto finish;
                    argptr = argend + 1;
                }
                argend++;
            }
            if ((argend - argptr) > 0)
            {
                while (*argptr == ' ')
                    argptr++;
                status = append(result->args, argptr);
                if (status != U1DB_OK)
                    goto finish;
            }
        }
        status = init_transformation(&inner);
        if (status != U1DB_OK)
            goto finish;
        status = parse(new_ptr, inner);
        if (status != U1DB_OK)
        {
            destroy_transformation(inner);
            goto finish;
        }
        result->next = inner;
    } else {
        if (*new_ptr != '\0')
        {
            status = U1DB_UNHANDLED_CHARACTERS;
            goto finish;
        }
        if (strlen(word) == 0)
        {
            status = U1DB_MISSING_FIELD_SPECIFIER;
            goto finish;
        }
        if (word[strlen(word) - 1] == '.')
        {
            status = U1DB_INVALID_FIELD_SPECIFIER;
            goto finish;
        }
        result->op = *extract_field_values;
        status = split(result->args, word, '.');
    }
finish:
    free(word);
    free(field_copy);
    free(new_field);
    return status;
}

static int
evaluate_index_and_insert_into_db(void *context, const char *expression)
{
    struct evaluate_index_context *ctx;
    transformation *tr = NULL;
    string_list *values = NULL;
    string_list_item *item = NULL;
    int status = U1DB_OK;

    ctx = (struct evaluate_index_context *)context;
    if (ctx->obj == NULL || !json_object_is_type(ctx->obj, json_type_object)) {
        return U1DB_INVALID_JSON;
    }
    status = init_transformation(&tr);
    if (status != U1DB_OK)
        goto finish;
    status = parse(expression, tr);
    if (status != U1DB_OK)
        goto finish;
    if ((status = init_list(&values)) != U1DB_OK)
        goto finish;
    status = apply_transformation(tr, ctx->obj, values);
    for (item = values->head; item != NULL; item = item->next)
    {
        if ((status = add_to_document_fields(ctx->db, ctx->doc_id, expression,
                        item->data)) != U1DB_OK)
            goto finish;
    }
finish:
    destroy_list(values);
    destroy_transformation(tr);
    return status;
}

// Is this expression field already in the indexed list?
// We make an assumption that the number of new expressions is always small
// relative to what is already indexed (which should be reasonably accurate).
static int
is_present(u1database *db, const char *expression, int *present)
{
    sqlite3_stmt *statement = NULL;
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
    const char **tmp = NULL;

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
    sqlite3_stmt *statement = NULL;
    struct evaluate_index_context context = {0};

    status = sqlite3_prepare_v2(db->sql_handle,
        "SELECT doc_id, content FROM document", -1,
        &statement, NULL);
    if (status != SQLITE_OK) {
        return status;
    }
    context.db = db;
    status = sqlite3_step(statement);
    while (status == SQLITE_ROW) {
        if (context.obj != NULL) {
            json_object_put(context.obj);
            context.obj = NULL;
        }
        context.doc_id = (const char*)sqlite3_column_text(statement, 0);
        context.content = (const char*)sqlite3_column_text(statement, 1);
        if (context.content == NULL)
        {
            // This document is deleted so does not need to be indexed.
            status = sqlite3_step(statement);
            continue;
        }
        context.obj = json_tokener_parse(context.content);
        if (context.obj == NULL
                || !json_object_is_type(context.obj, json_type_object))
        {
            // Invalid JSON in the database, for now we just continue?
            // TODO: Raise an error here.
            status = sqlite3_step(statement);
            continue;
        }
        for (i = 0; i < n_expressions; ++i) {
            status = evaluate_index_and_insert_into_db(&context,
                    expressions[i]);
            if (status != U1DB_OK)
                goto finish;
        }
        status = sqlite3_step(statement);
    }
    if (status == SQLITE_DONE) {
        status = U1DB_OK;
    }
finish:
    if (context.obj != NULL) {
        json_object_put(context.obj);
        context.obj = NULL;
    }
    sqlite3_finalize(statement);
    return status;
}
