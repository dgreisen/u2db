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
#include <json/json.h>
#include <curl/curl.h>


static int st_http_get_sync_info(u1db_sync_target *st,
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
static void st_http_finalize(u1db_sync_target *st);


static struct _http_state {
    char *base_url;
};

int
u1db__create_http_sync_target(const char *url, u1db_sync_target **target)
{
    int status = U1DB_OK;
    int url_len;
    struct _http_state *state;

    if (url == NULL || target == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    *target = (u1db_sync_target *)calloc(1, sizeof(u1db_sync_target));
    if (*target == NULL) {
        return U1DB_NOMEM;
    }
    state = (struct _http_state *)calloc(1, sizeof(struct _http_state));
    if (state == NULL) {
        free(*target);
        *target = NULL;
        return U1DB_NOMEM;
    }
    // Copy the url, but ensure that it ends in a '/'
    url_len = strlen(url);
    if (url[url_len-1] == '/') {
        state->base_url = strdup(url);
    } else {
        state->base_url = (char*)calloc(url_len+2, sizeof(char));
        memcpy(state->base_url, url, url_len);
        state->base_url[url_len] = '/';
        state->base_url[url_len+1] = '\0';
    }
    (*target)->implementation = state;
    (*target)->get_sync_info = st_http_get_sync_info;
    (*target)->record_sync_info = st_http_record_sync_info;
    (*target)->sync_exchange = st_http_sync_exchange;
    (*target)->get_sync_exchange = st_http_get_sync_exchange;
    (*target)->finalize_sync_exchange = st_http_finalize_sync_exchange;
    (*target)->_set_trace_hook = st_http_set_trace_hook;
    (*target)->finalize = st_http_finalize;
    return status;
}


static int
st_http_get_sync_info(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen)
{
    struct _http_state *state;
    CURL *curl;
    CURLcode code;

    state = (struct _http_state *)st->implementation;

    curl = curl_easy_init();
    code = curl_easy_setopt(curl, CURLOPT_URL, "http://example.com");
    code = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_cleanup(curl);
}


static int
st_http_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen)
{
    return U1DB_NOT_IMPLEMENTED;
}


static int
st_http_sync_exchange(u1db_sync_target *st, u1database *source_db,
        int n_doc_ids, const char **doc_ids, int *generations,
        int *target_gen, void *context, u1db_doc_gen_callback cb)
{
    return U1DB_NOT_IMPLEMENTED;
}


static int
st_http_get_sync_exchange(u1db_sync_target *st,
                         const char *source_replica_uid,
                         int source_gen,
                         u1db_sync_exchange **exchange)
{
    return U1DB_NOT_IMPLEMENTED;
}


static void
st_http_finalize_sync_exchange(u1db_sync_target *st,
                               u1db_sync_exchange **exchange)
{
}


static int
st_http_set_trace_hook(u1db_sync_target *st, void *context,
                       u1db__trace_callback cb)
{
    st->trace_context = context;
    st->trace_cb = cb;
    return U1DB_OK;
}


static void
st_http_finalize(u1db_sync_target *st)
{
    if (st->implementation != NULL) {
        struct _http_state *state;
        state = (struct _http_state *)st->implementation;
        if (state->base_url != NULL) {
            free(state->base_url);
            state->base_url = NULL;
        }
        free(st->implementation);
        st->implementation = NULL;
    }
}


int
u1db__format_get_sync_info_url(u1db_sync_target *st,
                               const char *source_replica_uid, char **sync_url)
{
    int url_len;
    struct _http_state *state;
    char *tmp;
    CURL *curl;

    state = (struct _http_state *)st->implementation;

    url_len = strlen(state->base_url);
    url_len += strlen("sync-from/");
    curl = curl_easy_init();
    tmp = curl_easy_escape(curl, source_replica_uid, 0);
    url_len += strlen(tmp);

    *sync_url = (char *)calloc(url_len+1, sizeof(char));
    snprintf(*sync_url, url_len, "%ssync-from/%s", state->base_url, tmp);
    curl_easy_cleanup(curl);
    curl_free(tmp);

    return U1DB_OK;
}
