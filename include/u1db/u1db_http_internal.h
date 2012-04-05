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

#ifndef _U1DB_HTTP_INTERNAL_H_
#define _U1DB_HTTP_INTERNAL_H_

#include "u1db/u1db_internal.h"


int u1db__format_sync_url(u1db_sync_target *st,
        const char *source_replica_uid, char **sync_url);

int u1db__set_oauth_credentials(u1db_sync_target *st,
    const char *consumer_key, const char *consumer_secret,
    const char *token_key, const char *token_secret);

#endif // _U1DB_HTTP_INTERNAL_H_
