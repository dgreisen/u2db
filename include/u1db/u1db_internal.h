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
    int num_entries;
    int max_num_entries;
    const char ***entries;
    void *buffer;
};

/**
 * Internal API, Get the global database rev.
 */
int u1db__get_db_rev(u1database *db, int *db_rev);

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

const char *u1db__schema[];
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

#endif // U1DB_INTERNAL_H
