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

#include "u1db/u1db_http_internal.h"
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
    CURL *curl;
};

static struct _http_return {
    int num_header_bytes;
    int max_header_bytes;
    char *header_buffer;
    int num_body_bytes;
    int max_body_bytes;
    char *body_buffer;
};


int
u1db__create_http_sync_target(const char *url, u1db_sync_target **target)
{
    int status = U1DB_OK;
    int url_len;
    struct _http_state *state;
    u1db_sync_target *new_target;

    if (url == NULL || target == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    new_target = (u1db_sync_target *)calloc(1, sizeof(u1db_sync_target));
    if (new_target == NULL) { goto oom; }
    state = (struct _http_state *)calloc(1, sizeof(struct _http_state));
    if (state == NULL) { goto oom; }
    state->curl = curl_easy_init();
    if (state->curl == NULL) { goto oom; }
    // All conversations are done without CURL generating progress bars.
    status = curl_easy_setopt(state->curl, CURLOPT_NOPROGRESS, 1L);
    if (status != CURLE_OK) { goto fail; }
    // Copy the url, but ensure that it ends in a '/'
    url_len = strlen(url);
    if (url[url_len-1] == '/') {
        state->base_url = strdup(url);
        if (state->base_url == NULL) { goto oom; }
    } else {
        state->base_url = (char*)calloc(url_len+2, sizeof(char));
        if (state->base_url == NULL) { goto oom; }
        memcpy(state->base_url, url, url_len);
        state->base_url[url_len] = '/';
        state->base_url[url_len+1] = '\0';
    }
    new_target->implementation = state;
    new_target->get_sync_info = st_http_get_sync_info;
    new_target->record_sync_info = st_http_record_sync_info;
    new_target->sync_exchange = st_http_sync_exchange;
    new_target->get_sync_exchange = st_http_get_sync_exchange;
    new_target->finalize_sync_exchange = st_http_finalize_sync_exchange;
    new_target->_set_trace_hook = st_http_set_trace_hook;
    new_target->finalize = st_http_finalize;
    *target = new_target;
    return status;
oom:
    status = U1DB_NOMEM;
fail:
    if (state != NULL) {
        if (state->base_url != NULL) {
            free(state->base_url);
            state->base_url = NULL;
        }
        if (state->curl != NULL) {
            curl_easy_cleanup(state->curl);
            state->curl = NULL;
        }
        free(state);
        state = NULL;
    }
    if (new_target != NULL) {
        free(new_target);
        new_target = NULL;
    }
    return status;
}


static size_t
recv_header_bytes(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    int needed_bytes;
    struct _http_return *ret;
    char *tmp_buf;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    // Note: curl says that CURLOPT_HEADERFUNCTION is called 1 time for each
    //       header, with exactly the header contents. So we should be able to
    //       change this into something that parses the header content itself,
    //       without separately buffering the raw bytes.
    ret = (struct _http_return *)userdata;
    total_bytes = size * nmemb;
    needed_bytes = ret->num_header_bytes + total_bytes + 1;
    tmp_buf = calloc(total_bytes + 1, 1);
    memcpy(tmp_buf, ptr, total_bytes);
    free(tmp_buf);
    if (needed_bytes >= ret->max_header_bytes) {
        ret->max_header_bytes = max((ret->max_header_bytes * 2), needed_bytes);
        ret->max_header_bytes += 100;
        ret->header_buffer = realloc(ret->header_buffer, ret->max_header_bytes);
    }
    memcpy(ret->header_buffer + ret->num_header_bytes, ptr, total_bytes);
    ret->num_header_bytes += total_bytes;
    ret->header_buffer[ret->num_header_bytes + 1] = '\0';
    return total_bytes;
}


static size_t
recv_body_bytes(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    int needed_bytes;
    struct _http_return *ret;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    ret = (struct _http_return *)userdata;
    total_bytes = size * nmemb;
    needed_bytes = ret->num_body_bytes + total_bytes + 1;
    if (needed_bytes >= ret->max_body_bytes) {
        ret->max_body_bytes = max((ret->max_body_bytes * 2), needed_bytes);
        ret->max_body_bytes += 100;
        ret->body_buffer = realloc(ret->body_buffer, ret->max_body_bytes);
    }
    memcpy(ret->body_buffer + ret->num_body_bytes, ptr, total_bytes);
    ret->num_body_bytes += total_bytes;
    ret->header_buffer[ret->num_header_bytes + 1] = '\0';
    return total_bytes;
}


static int
st_http_get_sync_info(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen)
{
    struct _http_state *state;
    struct _http_return ret = {0};
    char *url = NULL;
    int status;
    long http_code;
    json_object *json = NULL, *obj = NULL;

    if (st == NULL || source_replica_uid == NULL || st_replica_uid == NULL
            || st_gen == NULL || source_gen == NULL
            || st->implementation == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    state = (struct _http_state *)st->implementation;
    if (state->curl == NULL) {
        return U1DB_INVALID_PARAMETER;
    }

    status = u1db__format_get_sync_info_url(st, source_replica_uid, &url);
    if (status != U1DB_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_URL, url);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HEADERFUNCTION,
                              recv_header_bytes);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HEADERDATA, &ret);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_WRITEFUNCTION,
                              recv_body_bytes);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_WRITEDATA, &ret);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_perform(state->curl);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (status != CURLE_OK) { goto finish; }
    if (http_code != 200) {
        status = http_code;
        goto finish;
    }
    json = json_tokener_parse(ret.body_buffer);
    if (json == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    obj = json_object_object_get(json, "target_replica_uid");
    if (obj == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    *st_replica_uid = strdup(json_object_get_string(obj));
    if (*st_replica_uid == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    json_object_put(obj);
    obj = json_object_object_get(json, "target_replica_generation");
    if (obj == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    *st_gen = json_object_get_int(obj);
    json_object_put(obj);
    obj = json_object_object_get(json, "source_replica_generation");
    if (obj == NULL) {
        status = U1DB_INVALID_HTTP_RESPONSE;
        goto finish;
    }
    *source_gen = json_object_get_int(obj);
    json_object_put(obj);
finish:
    if (ret.header_buffer != NULL) {
        free(ret.header_buffer);
    }
    if (ret.body_buffer != NULL) {
        free(ret.body_buffer);
    }
    if (json != NULL) {
        json_object_put(json);
    }
    if (url != NULL) {
        free(url);
    }
    return status;
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
        if (state->curl != NULL) {
            curl_easy_cleanup(state->curl);
            state->curl = NULL;
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

    state = (struct _http_state *)st->implementation;

    url_len = strlen(state->base_url) + 1;
    url_len += strlen("sync-from/");
    tmp = curl_easy_escape(state->curl, source_replica_uid, 0);
    url_len += strlen(tmp);

    *sync_url = (char *)calloc(url_len+1, sizeof(char));
    snprintf(*sync_url, url_len, "%ssync-from/%s", state->base_url, tmp);
    curl_free(tmp);

    return U1DB_OK;
}
