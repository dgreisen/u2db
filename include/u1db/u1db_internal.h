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

#ifndef U1DB_INTERNAL_H
#define U1DB_INTERNAL_H

#include <stdarg.h>
#include "u1db/u1db.h"
#include "u1db/compat.h"

typedef struct sqlite3 sqlite3;

struct _u1database
{
    sqlite3 *sql_handle;
    char *replica_uid;
};

struct _u1query {
    const char *index_name;
    int num_fields;
    char **fields;
};

typedef struct _u1db_sync_exchange u1db_sync_exchange;

typedef struct _u1db_sync_target u1db_sync_target;

struct _u1db_sync_target {
    u1database *db;
    /**
     * Get the information for synchronization about another replica.
     *
     * @param st    Pass this sync_target to the function,
     *              eg st->get_sync_info(st, ...)
     * @param source_replica_uid    The unique identifier for the source we
     *                              want to synchronize from.
     * @param st_replica_uid    (OUT) The replica uid for the database this
     *                          SyncTarget is attached to.
     *                          Note that this is const char and memory will be
     *                          managed by the sync_target, so it should *not*
     *                          be freed.
     * @param st_get            (OUT) The database generation for this sync
     *                          target, matches st_replica_uid
     * @param source_gen        (OUT) The last generation of source_replica_uid
     *                          that st has synchronized with.
     */
    int (*get_sync_info)(u1db_sync_target *st,
        const char *source_replica_uid,
        const char **st_replica_uid, int *st_gen, int *source_gen);
    /**
     * Set the synchronization information about another replica.
     *
     * @param st    Pass this sync_target to the function,
     *              eg st->get_sync_info(st, ...)
     * @param source_replica_uid    The unique identifier for the source we
     *                              want to synchronize from.
     * @param source_gen        The last generation of source_replica_uid
     *                          that st has synchronized with.
     */
    int (*record_sync_info)(u1db_sync_target *st,
        const char *source_replica_uid, int source_gen);

    /**
     * Create a sync_exchange state object.
     *
     * This encapsulates the state during a single document exchange.
     * The returned u1db_sync_exchange object should be freed with
     * finalize_sync_exchange.
     */
    int (*get_sync_exchange)(u1db_sync_target *st,
                             const char *source_replica_uid,
                             u1db_sync_exchange **exchange);

    void (*finalize_sync_exchange)(u1db_sync_target *st,
                                   u1db_sync_exchange **exchange);
};

struct _u1db_sync_exchange {
    u1database *db;
    const char *source_replica_uid;
    int new_gen;
    //     self.seen_ids = set()  # incoming ids not superseded
    //     self.changes_to_return = None
    //     # for tests
    //     self._incoming_trace = []
    //     self._db._last_exchange_log = {
    //         'receive': {'docs': self._incoming_trace},
    //         'return': None
    //         }
};

/**
 * Internal API, Get the global database rev.
 */
int u1db__get_db_generation(u1database *db, int *generation);

/**
 * Internal API, Allocate a new document id, for cases when callers do not
 * supply their own. Callers of this API are expected to free the result.
 */
char *u1db__allocate_doc_id(u1database *db);

/**
 * Internal api, close the underlying sql instance.
 */
int u1db__sql_close(u1database *db);

/**
 * Internal api, check to see if the underlying SQLite handle has been closed.
 */
int u1db__sql_is_open(u1database *db);

/**
 * Check if a doc_id is valid.
 *
 * Returns U1DB_OK if everything is ok, otherwise U1DB_INVALID_DOC_ID.
 */
int u1db__is_doc_id_valid(const char *doc_id);

/**
 * Internal api, run an SQL query directly.
 */
typedef struct _u1db_row {
    struct _u1db_row *next;
    int num_columns;
    int *column_sizes;
    unsigned char **columns;
} u1db_row;

typedef struct _u1db_table {
    int status;
    u1db_row *first_row;
} u1db_table;

u1db_table *u1db__sql_run(u1database *db, const char *sql, size_t n);
void u1db__free_table(u1db_table **table);


/**
 * Get the list of everything that has changed that we've recorded.
 */
int u1db__get_transaction_log(u1database *db, void *context,
                              int (*cb)(void *context, char *doc_id, int gen));

/**
 * Get the known generation we synchronized with another implementation.
 *
 * @param replica_uid The identifier for the other database
 * @param generation  (OUT) The last generation that we know we synchronized
 *                    with the other database.
 */
int u1db__get_sync_generation(u1database *db, const char *replica_uid,
                              int *generation);

/**
 * Set the known sync generation for another replica.
 *
 */
int u1db__set_sync_generation(u1database *db, const char *replica_uid,
                              int generation);

/**
 * Internal sync api, get the stored information about another machine.
 */
int u1db__sync_get_machine_info(u1database *db, const char *other_replica_uid,
                            int *other_db_rev, char **my_replica_uid,
                            int *my_db_rev);

/**
 * Internal sync api, store information about another machine.
 */
int u1db__sync_record_machine_info(u1database *db, const char *replica_uid,
                                   int db_rev);

const char **u1db__schema;
const int u1db__schema_len;

typedef struct _u1db_record {
    struct _u1db_record *next;
    char *doc_id;
    char *doc_rev;
    char *doc;
} u1db_record;

/**
 * Internal sync api, exchange sync records.
 */
int u1db__sync_exchange(u1database *db, const char *from_replica_uid,
                        int from_db_rev, int last_known_rev,
                        u1db_record *from_records, u1db_record **new_records,
                        u1db_record **conflict_records);

/**
 * Allocate a new u1db_record, and copy all records over.
 */
u1db_record *u1db__create_record(const char *doc_id, const char *doc_rev,
                                 const char *doc);

u1db_record *u1db__copy_record(u1db_record *src);

/**
 * Free a linked list of records. All linked records will be freed, including
 * all memory referenced from them.
 */
void u1db__free_records(u1db_record **record);

/**
 * Create a new u1db_document object. This should be freed
 */
u1db_document *u1db__allocate_document(const char *doc_id, const char *revision,
                                       const char *content, int has_conflicts);

/**
 * Generate a unique id.
 *
 * @param uuid A buffer to put the id, must be 32 bytes long.
 */
int u1db__generate_hex_uuid(char *uuid);


/**
 * Format a given query.
 *
 * @param n_fields  The number of fields being passed in, (the number of args
 * 		    in argp)
 * @param argp	    Arguments being passed. It is assumed that all arguments
 * 		    will be of type "char*".
 * @param buf (OUT) The character array. This will be dynamically allocated,
 * 		    and callers must free() it.
 * @param wildcard (IN/OUT) Any array indicating a wildcard type for this
 * 			 argument. A 0 indicates this is an exact match, a 1
 * 			 indicates this is a pure wildcard (eg, "*") and a 2
 * 			 indicates this is a glob (eg "f*").
 * 			 This must point to an array at least n_fields wide.
 */
int u1db__format_query(int n_fields, va_list argp, char **buf, int *wildcard);

/**
 * Given this document content, update the indexed fields in the db.
 */
int u1db__update_indexes(u1database *db, const char *doc_id,
                         const char *content);

/**
 * Find what expressions do not already exist in the database.
 *
 * @param n_unique (OUT) The number of unique expressions found
 * @param unique_expressions (OUT) An array holding pointers to the strings in
 *                       expressions, must be freed by the caller if there
 *                       isn't an error.
 */
int u1db__find_unique_expressions(u1database *db,
                              int n_expressions, const char **expressions,
                              int *n_unique, const char ***unique_expressions);

/**
 * Add the given field expressions to be indexed.
 *
 * This will iterate over all documents, and request that they be indexed.
 */
int u1db__index_all_docs(u1database *db, int n_expressions,
                         const char **expressions);


/**
 * Create an object for synchronizing.
 *
 * The object created should be freed using u1db__free_sync_target. It holds a
 * pointer to the database that created it, so you must keep the database
 * object alive as long as the synchronization object is alive.
 */
int u1db__get_sync_target(u1database *db, u1db_sync_target **sync_target);


void u1db__free_sync_target(u1db_sync_target **sync_target);


int u1db__sync_exchange_insert_doc_from_source(u1db_sync_exchange *se,
        u1db_document *doc, int source_gen);
#endif // U1DB_INTERNAL_H
