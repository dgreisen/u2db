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
static int initialize_curl(struct _http_state *state);


static struct _http_state {
    char *base_url;
    CURL *curl;
    struct curl_slist *headers;
};

static struct _http_request {
    struct _http_state *state;
    int num_header_bytes;
    int max_header_bytes;
    char *header_buffer;
    int num_body_bytes;
    int max_body_bytes;
    char *body_buffer;
    int num_put_bytes;
    const char *put_buffer;
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
    status = initialize_curl(state);
    if (status != U1DB_OK) { goto fail; }
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
    struct _http_request *req;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    // Note: curl says that CURLOPT_HEADERFUNCTION is called 1 time for each
    //       header, with exactly the header contents. So we should be able to
    //       change this into something that parses the header content itself,
    //       without separately buffering the raw bytes.
    req = (struct _http_request *)userdata;
    total_bytes = size * nmemb;
    if (req->state != NULL && total_bytes > 9 && strncmp(ptr, "HTTP/", 5) == 0)
    {
        if (strncmp(ptr, "HTTP/1.0 ", 9) == 0) {
            // The server is an HTTP 1.0 server (like in the test suite). Tell
            // curl to treat it as such from now on. I don't understand why
            // curl isn't doing this already, because it has seen that the
            // server is v1.0
            curl_easy_setopt(req->state->curl, CURLOPT_HTTP_VERSION,
                             CURL_HTTP_VERSION_1_0);
        } else if (strncmp(ptr, "HTTP/1.1 ", 9) == 0) {
            curl_easy_setopt(req->state->curl, CURLOPT_HTTP_VERSION,
                             CURL_HTTP_VERSION_1_0);
        }
    }
    needed_bytes = req->num_header_bytes + total_bytes + 1;
    if (needed_bytes >= req->max_header_bytes) {
        req->max_header_bytes = max((req->max_header_bytes * 2), needed_bytes);
        req->max_header_bytes += 100;
        req->header_buffer = realloc(req->header_buffer, req->max_header_bytes);
    }
    memcpy(req->header_buffer + req->num_header_bytes, ptr, total_bytes);
    req->num_header_bytes += total_bytes;
    req->header_buffer[req->num_header_bytes + 1] = '\0';
    return total_bytes;
}


static size_t
recv_body_bytes(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    int needed_bytes;
    struct _http_request *req;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    req = (struct _http_request *)userdata;
    total_bytes = size * nmemb;
    needed_bytes = req->num_body_bytes + total_bytes + 1;
    if (needed_bytes >= req->max_body_bytes) {
        req->max_body_bytes = max((req->max_body_bytes * 2), needed_bytes);
        req->max_body_bytes += 100;
        req->body_buffer = realloc(req->body_buffer, req->max_body_bytes);
    }
    memcpy(req->body_buffer + req->num_body_bytes, ptr, total_bytes);
    req->num_body_bytes += total_bytes;
    req->header_buffer[req->num_header_bytes + 1] = '\0';
    return total_bytes;
}


static size_t
send_put_bytes(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total_bytes;
    struct _http_request *req;
    if (userdata == NULL) {
        // No bytes processed, because we have nowhere to put them
        return 0;
    }
    req = (struct _http_request *)userdata;
    total_bytes = size * nmemb;
    if (total_bytes > (size_t) req->num_put_bytes) {
        total_bytes = req->num_put_bytes;
    }
    memcpy(ptr, req->put_buffer, total_bytes);
    req->num_put_bytes -= total_bytes;
    req->put_buffer += total_bytes;
    return total_bytes;
}


static int
initialize_curl(struct _http_state *state)
{
    int status;

    state->curl = curl_easy_init();
    if (state->curl == NULL) { goto oom; }
    // All conversations are done without CURL generating progress bars.
    status = curl_easy_setopt(state->curl, CURLOPT_NOPROGRESS, 1L);
    if (status != CURLE_OK) { goto fail; }
    /// status = curl_easy_setopt(state->curl, CURLOPT_VERBOSE, 1L);
    /// if (status != CURLE_OK) { goto fail; }
    state->headers = curl_slist_append(NULL, "Content-Type: application/json");
    if (state->headers == NULL) {
        status = U1DB_NOMEM;
        goto fail;
    }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPHEADER, state->headers);
    if (status != CURLE_OK) { goto fail; }
    status = curl_easy_setopt(state->curl, CURLOPT_HEADERFUNCTION,
                              recv_header_bytes);
    if (status != CURLE_OK) { goto fail; }
    status = curl_easy_setopt(state->curl, CURLOPT_WRITEFUNCTION,
                              recv_body_bytes);
    if (status != CURLE_OK) { goto fail; }
    status = curl_easy_setopt(state->curl, CURLOPT_READFUNCTION,
                              send_put_bytes);
    if (status != CURLE_OK) { goto fail; }
    return status;
oom:
    status = U1DB_NOMEM;
fail:
    if (state->curl != NULL) {
        curl_easy_cleanup(state->curl);
        state->curl = NULL;
    }
    if (state->headers != NULL) {
        curl_slist_free_all(state->headers);
    }
    return status;
}


static int
st_http_get_sync_info(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen)
{
    struct _http_state *state;
    struct _http_request req = {0};
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

    req.state = state;
    status = u1db__format_sync_info_url(st, source_replica_uid, &url);
    if (status != U1DB_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPGET, 1L);
    if (status != CURLE_OK) { goto finish; }
    // status = curl_easy_setopt(state->curl, CURLOPT_USERAGENT, "...");
    status = curl_easy_setopt(state->curl, CURLOPT_URL, url);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPHEADER, state->headers);
    if (status != CURLE_OK) { goto finish; }
    status = simple_set_curl_data(state->curl, &req, &req, NULL);
    if (status != CURLE_OK) { goto finish; }
    // Now do the GET
    status = curl_easy_perform(state->curl);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (status != CURLE_OK) { goto finish; }
    if (http_code != 200) { // 201 for created? shouldn't happen on GET
        status = http_code;
        goto finish;
    }
    json = json_tokener_parse(req.body_buffer);
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
    if (req.header_buffer != NULL) {
        free(req.header_buffer);
    }
    if (req.body_buffer != NULL) {
        free(req.body_buffer);
    }
    if (json != NULL) {
        json_object_put(json);
    }
    if (url != NULL) {
        free(url);
    }
    return status;
}


// Use the default send_put_bytes, recv_body_bytes, and recv_header_bytes. Only
// set the functions if the associated data is not NULL
static int
simple_set_curl_data(CURL *curl, struct _http_request *header,
                     struct _http_request *body, struct _http_request *put)
{
    int status;
    status = curl_easy_setopt(curl, CURLOPT_HEADERDATA, header);
    if (status != CURLE_OK) { goto finish; }
    if (header == NULL) {
        status = curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, NULL);
    } else {
        status = curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                                  recv_header_bytes);
    }
    status = curl_easy_setopt(curl, CURLOPT_WRITEDATA, body);
    if (status != CURLE_OK) { goto finish; }
    if (body == NULL) {
        status = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, NULL);
    } else {
        status = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                                  recv_body_bytes);
    }
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(curl, CURLOPT_READDATA, put);
    if (status != CURLE_OK) { goto finish; }
    if (put == NULL) {
        status = curl_easy_setopt(curl, CURLOPT_READFUNCTION, NULL);
    } else {
        status = curl_easy_setopt(curl, CURLOPT_READFUNCTION,
                                  send_put_bytes);
    }
finish:
    return status;
}


static int
st_http_record_sync_info(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen)
{
    struct _http_state *state;
    struct _http_request req = {0};
    char *url = NULL;
    int status;
    long http_code;
    json_object *json = NULL;
    const char *raw_body = NULL;
    int raw_len;
    struct curl_slist *headers = NULL;

    if (st == NULL || source_replica_uid == NULL || st->implementation == NULL)
    {
        return U1DB_INVALID_PARAMETER;
    }
    state = (struct _http_state *)st->implementation;
    if (state->curl == NULL) {
        return U1DB_INVALID_PARAMETER;
    }

    status = u1db__format_sync_info_url(st, source_replica_uid, &url);
    if (status != U1DB_OK) { goto finish; }
    json = json_object_new_object();
    if (json == NULL) {
        status = U1DB_NOMEM;
        goto finish;
    }
    json_object_object_add(json, "generation", json_object_new_int(source_gen));
    raw_body = json_object_to_json_string(json);
    raw_len = strlen(raw_body);
    req.state = state;
    req.put_buffer = raw_body;
    req.num_put_bytes = raw_len;

    status = curl_easy_setopt(state->curl, CURLOPT_URL, url);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_HTTPHEADER, state->headers);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_UPLOAD, 1L);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_PUT, 1L);
    if (status != CURLE_OK) { goto finish; }
    status = simple_set_curl_data(state->curl, &req, &req, &req);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_setopt(state->curl, CURLOPT_INFILESIZE_LARGE,
                              (curl_off_t)req.num_put_bytes);
    if (status != CURLE_OK) { goto finish; }

    // Now actually send the data
    status = curl_easy_perform(state->curl);
    if (status != CURLE_OK) { goto finish; }
    status = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (status != CURLE_OK) { goto finish; }
    if (http_code != 200 && http_code != 201) {
        status = http_code;
        goto finish;
    }
finish:
    if (req.header_buffer != NULL) {
        free(req.header_buffer);
    }
    if (req.body_buffer != NULL) {
        free(req.body_buffer);
    }
    if (json != NULL) {
        json_object_put(json);
    }
    if (url != NULL) {
        free(url);
    }
    if (headers != NULL) {
    }
    return status;
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
        if (state->headers != NULL) {
            curl_slist_free_all(state->headers);
            state->headers = NULL;
        }
        free(st->implementation);
        st->implementation = NULL;
    }
}


int
u1db__format_sync_info_url(u1db_sync_target *st,
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
