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
    status = u1db__get_db_generation(st->db, st_gen);
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
    if (st == NULL || source_replica_uid == NULL || exchange == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    *exchange = (u1db_sync_exchange *)calloc(1, sizeof(u1db_sync_exchange));
    if (*exchange == NULL) {
        return U1DB_NOMEM;
    }
    (*exchange)->db = st->db;
    (*exchange)->source_replica_uid = source_replica_uid;
    (*exchange)->last_known_source_gen = source_gen;
    return U1DB_OK;
}


static void
st_finalize_sync_exchange(u1db_sync_target *st, u1db_sync_exchange **exchange)
{
    if (exchange == NULL || *exchange == NULL) {
        return;
    }
    free(*exchange);
    *exchange = NULL;
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
        // TODO: Update u1db_sync_exchange.seen_ids
        //       One option is to use sqlite TEMP TABLE as a set...
    } else {
        // state should be either U1DB_SUPERSEDED or U1DB_CONFLICTED, in either
        // case, we don't count this as a 'seen_id' because we will want to be
        // returning a document with this identifier back to the user.
    }
    return status;
}


int
u1db__sync_exchange_find_and_return_docs(u1db_sync_exchange *se)
{
    return U1DB_NOT_IMPLEMENTED;
}
