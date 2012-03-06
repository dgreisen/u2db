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
#include <string.h>
#include <json/linkhash.h>


static int st_get_sync_info (u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen);

static int st_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen);

static int st_get_sync_exchange(u1db_sync_target *st,
                         const char *source_replica_uid,
                         int source_gen,
                         u1db_sync_exchange **exchange);

static void st_finalize_sync_exchange(u1db_sync_target *st,
                               u1db_sync_exchange **exchange);
static int st_set_trace_hook(u1db_sync_target *st,
                             void *context, u1db__trace_callback cb);
static void se_free_seen_id(struct lh_entry *e);


int
u1db__get_sync_target(u1database *db, u1db_sync_target **sync_target)
{
    int status = U1DB_OK;

    if (db == NULL || sync_target == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    *sync_target = (u1db_sync_target *)calloc(1, sizeof(u1db_sync_target));
    if (*sync_target == NULL) {
        return U1DB_NOMEM;
    }
    (*sync_target)->db = db;
    (*sync_target)->get_sync_info = st_get_sync_info;
    (*sync_target)->record_sync_info = st_record_sync_info;
    (*sync_target)->get_sync_exchange = st_get_sync_exchange;
    (*sync_target)->finalize_sync_exchange = st_finalize_sync_exchange;
    (*sync_target)->_set_trace_hook = st_set_trace_hook;
    return status;
}


void
u1db__free_sync_target(u1db_sync_target **sync_target)
{
    if (sync_target == NULL || *sync_target == NULL) {
        return;
    }
    free(*sync_target);
    *sync_target = NULL;
}


static int
st_get_sync_info(u1db_sync_target *st, const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen)
{
    int status = U1DB_OK;
    if (st == NULL || source_replica_uid == NULL || st_replica_uid == NULL
            || st_gen == NULL || source_gen == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    // TODO: This really feels like it should be done inside some sort of
    //       transaction, so that the sync information is consistent with the
    //       current db generation. (at local generation X we are synchronized
    //       with remote generation Y.)
    //       At the very least, though, we check the sync generation *first*,
    //       so that we should only be getting the same data again, if for some
    //       reason we are currently synchronizing with the remote object.
    status = u1db_get_replica_uid(st->db, st_replica_uid);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__get_sync_generation(st->db, source_replica_uid, source_gen);
    if (status != U1DB_OK) { goto finish; }
    status = u1db__get_generation(st->db, st_gen);
finish:
    return status;
}


static int
st_record_sync_info(u1db_sync_target *st, const char *source_replica_uid,
                    int source_gen)
{
    if (st == NULL || source_replica_uid == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    return u1db__set_sync_generation(st->db, source_replica_uid, source_gen);
}


static int
st_get_sync_exchange(u1db_sync_target *st, const char *source_replica_uid,
                     int source_gen, u1db_sync_exchange **exchange)
{
    u1db_sync_exchange *tmp;
    if (st == NULL || source_replica_uid == NULL || exchange == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    tmp = (u1db_sync_exchange *)calloc(1, sizeof(u1db_sync_exchange));
    if (tmp == NULL) {
        return U1DB_NOMEM;
    }
    tmp->db = st->db;
    tmp->source_replica_uid = source_replica_uid;
    tmp->last_known_source_gen = source_gen;
    // Note: lh_table is overkill for what we need. We only need a set, not a
    //       mapping, and we don't need the prev/next pointers. But it is
    //       already available, and doesn't require us to implement and debug
    //       another set() implementation.
    tmp->seen_ids = lh_kchar_table_new(100, "seen_ids",
            se_free_seen_id);
    tmp->trace_context = st->trace_context;
    tmp->trace_cb = st->trace_cb;
    *exchange = tmp;
    return U1DB_OK;
}


static void
st_finalize_sync_exchange(u1db_sync_target *st, u1db_sync_exchange **exchange)
{
    int i;
    if (exchange == NULL || *exchange == NULL) {
        return;
    }
    if ((*exchange)->seen_ids != NULL) {
        lh_table_free((*exchange)->seen_ids);
        (*exchange)->seen_ids = NULL;
    }
    if ((*exchange)->doc_ids_to_return != NULL) {
        for (i = 0; i < (*exchange)->num_doc_ids; ++i) {
            free((*exchange)->doc_ids_to_return[i]);
        }
        free((*exchange)->doc_ids_to_return);
        (*exchange)->doc_ids_to_return = NULL;
        (*exchange)->num_doc_ids = 0;
    }
    if ((*exchange)->gen_for_doc_ids != NULL) {
        free((*exchange)->gen_for_doc_ids);
        (*exchange)->gen_for_doc_ids = NULL;
    }
    free(*exchange);
    *exchange = NULL;
}


static int
st_set_trace_hook(u1db_sync_target *st, void *context, u1db__trace_callback cb)
{
    st->trace_context = context;
    st->trace_cb = cb;
    return U1DB_OK;
}


static void
se_free_seen_id(struct lh_entry *e)
{
    if (e == NULL) {
        return;
    }
    if (e->k != NULL) {
        free((void *)e->k);
        e->k = NULL;
    }
    if (e->v != NULL) {
        free((void *)e->v);
        e->v = NULL;
    }
}


int
u1db__sync_exchange_seen_ids(u1db_sync_exchange *se, int *n_ids,
                             const char ***doc_ids)
{
    int i;
    struct lh_entry *entry;
    if (se == NULL || n_ids == NULL || doc_ids == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (se->seen_ids == NULL || se->seen_ids->count == 0) {
        *n_ids = 0;
        *doc_ids = NULL;
        return U1DB_OK;
    }
    *n_ids = se->seen_ids->count;
    (*doc_ids) = (const char **)calloc(*n_ids, sizeof(char *));
    i = 0;
    lh_foreach(se->seen_ids, entry) {
        if (entry->k != NULL) {
            if (i >= (*n_ids)) {
                // TODO: Better error? For some reason we found more than
                //       'count' valid entries
                return U1DB_INVALID_PARAMETER;
            }
            (*doc_ids)[i] = entry->k;
            i++;
        }
    }
    return U1DB_OK;
}

int
u1db__sync_exchange_insert_doc_from_source(u1db_sync_exchange *se,
        u1db_document *doc, int source_gen)
{
    int status = U1DB_OK;
    int state;
    if (se == NULL || se->db == NULL || doc == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    status = u1db_put_doc_if_newer(se->db, doc, 0, se->source_replica_uid,
                                   source_gen, &state);
    if (state == U1DB_INSERTED || state == U1DB_CONVERGED) {
        lh_table_insert(se->seen_ids, strdup(doc->doc_id),
                        strdup(doc->doc_rev));
    } else {
        // state should be either U1DB_SUPERSEDED or U1DB_CONFLICTED, in either
        // case, we don't count this as a 'seen_id' because we will want to be
        // returning a document with this identifier back to the user.
    }
    return status;
}


// Callback for whats_changed to map the callback into the sync_exchange
// doc_ids_to_return array.
static int
whats_changed_to_doc_ids(void *context, const char *doc_id, int gen)
{
    u1db_sync_exchange *state;
    state = (u1db_sync_exchange *)context;
    if (lh_table_lookup(state->seen_ids, doc_id) != NULL) {
        // This document was already seen, so we don't need to return it
        // TODO: See bug #944049
        return 0;
    }
    if (state->num_doc_ids >= state->max_doc_ids) {
        state->max_doc_ids = (state->max_doc_ids * 2) + 10;
        if (state->doc_ids_to_return == NULL) {
            state->doc_ids_to_return = (char **)calloc(state->max_doc_ids,
                                                       sizeof(char*));
            state->gen_for_doc_ids = (int *)calloc(state->max_doc_ids,
                                                   sizeof(int));
        } else {
            state->doc_ids_to_return = (char **)realloc(
                    state->doc_ids_to_return,
                    state->max_doc_ids * sizeof(char*));
            state->gen_for_doc_ids = (int *)realloc(state->gen_for_doc_ids,
                    state->max_doc_ids * sizeof(int));
        }
        if (state->doc_ids_to_return == NULL
                || state->gen_for_doc_ids == NULL)
        {
            return U1DB_NOMEM;
        }
    }
    state->doc_ids_to_return[state->num_doc_ids] = strdup(doc_id);
    state->gen_for_doc_ids[state->num_doc_ids] = gen;
    state->num_doc_ids++;
    return 0;
}


int
u1db__sync_exchange_find_doc_ids_to_return(u1db_sync_exchange *se)
{
    int status;
    if (se == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (se->trace_cb) {
        status = se->trace_cb(se->trace_context, "before whats_changed");
        if (status != U1DB_OK) { goto finish; }
    }
    status = u1db_whats_changed(se->db, &se->new_gen, (void*)se,
            whats_changed_to_doc_ids);
    if (se->trace_cb) {
        status = se->trace_cb(se->trace_context, "after whats_changed");
        if (status != U1DB_OK) { goto finish; }
    }
finish:
    return status;
}


struct _get_docs_to_return_docs_context {
    u1db_sync_exchange *se;
    int doc_offset;
    void *orig_context;
    int (*user_cb)(void *context, u1db_document *doc, int gen);
};


static int
get_docs_to_return_docs(void *context, u1db_document *doc)
{
    struct _get_docs_to_return_docs_context *ctx;
    int status;
    ctx = (struct _get_docs_to_return_docs_context *)context;
    // Note: using doc_offset in this way assumes that u1db_get_docs will
    //       always return them in exactly the order we requested. This is
    //       probably true, though.
    // TODO: We could check to make sure ctx->se...[].doc_id matches doc.doc_id
    status = ctx->user_cb(ctx->orig_context, doc,
            ctx->se->gen_for_doc_ids[ctx->doc_offset]);
    ctx->doc_offset++;
    return status;
}


int
u1db__sync_exchange_return_docs(u1db_sync_exchange *se, void *context,
        int (*cb)(void *context, u1db_document *doc, int gen))
{
    int status = U1DB_OK;
    struct _get_docs_to_return_docs_context local_ctx;
    if (se == NULL || cb == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    local_ctx.se = se;
    local_ctx.orig_context = context;
    local_ctx.user_cb = cb;
    local_ctx.doc_offset = 0;
    if (se->trace_cb) {
        status = se->trace_cb(se->trace_context, "before get_docs");
        if (status != U1DB_OK) { goto finish; }
    }
    if (se->num_doc_ids > 0) {
        // For some reason, gcc doesn't like to auto-cast "char **" to "const
        // char **".
        status = u1db_get_docs(se->db, se->num_doc_ids,
                (const char **)se->doc_ids_to_return,
                0, &local_ctx, get_docs_to_return_docs);
    }
finish:
    return status;
}
