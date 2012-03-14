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


static int st_http_get_sync_info (u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen);

static int st_http_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen);

static int st_http_sync_exchange(u1db_sync_target *st, u1database *source_db,
        int n_doc_ids, const char **doc_ids, int *generations,
        int *target_gen, void *context, u1db_doc_gen_callback cb);
static int st_http_get_sync_exchange(u1db_sync_target *st,
                         const char *source_replica_uid,
                         int source_gen,
                         u1db_sync_exchange **exchange);

static void st_http_finalize_sync_exchange(u1db_sync_target *st,
                               u1db_sync_exchange **exchange);
static int st_http_set_trace_hook(u1db_sync_target *st,
                             void *context, u1db__trace_callback cb);


int
u1db__create_http_sync_target(const char *url, u1db_sync_target **target)
{
}
