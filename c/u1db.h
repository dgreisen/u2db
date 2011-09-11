/*
 * Copyright 2011 Canonical Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3, as published
 * by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranties of
 * MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _U1DB_H_
#define _U1DB_H_

typedef struct _u1database u1database;

/**
 * The basic constructor for a new connection.
 */
u1database *u1db_open(const char *fname);

/**
 * Close an existing connection, freeing memory, etc.
 * This is generally used as u1db_free(&db);
 * After freeing the memory, we will set the pointer to NULL.
 */
void u1db_free(u1database **db);

/**
 * Set the machine_id defined for this database.
 */
int u1db_set_machine_id(u1database *db, const char *machine_id);

/**
 * Get the machine_id defined for this database.
 */
int u1db_get_machine_id(u1database *db, char **machine_id);

/**
 * Create a new document.
 *
 * @param doc: The JSON string representing the document.
 * @param n: The number of bytes in doc
 * @param doc_id: A string identifying the document. If the value supplied is
 *      NULL, then a new doc_id will be generated. Callers are responsible for
 *      then freeing the returned string.
 * @param doc_rev: The document revision. Callers are responsible for freeing
 *      the information.
 */
int u1db_create_doc(u1database *db, const char *doc, int n, char **doc_id,
                    char **doc_rev);

/**
 * Put new document content for the given document identifier.
 *
 * @param doc_id: A string identifying the document. If the value supplied is
 *      NULL, then a new doc_id will be generated. Callers are responsible for
 *      then freeing the returned string.
 * @param doc_rev: The document revision. This should contain the revision that
 *      is being replaced, and it will be filled in with the new document revision.
 *      The new revision will be malloced(), callers are responsible for
 *      calling free.
 * @param doc: The JSON string representing the document.
 * @param n: The number of bytes in doc
 */
int u1db_put_doc(u1database *db, const char *doc_id, char **doc_rev,
                 const char *doc, int n);

/**
 * Get the document defined by the given document id.
 *
 * @param doc_id (IN) The document we are looking for
 * @param doc_rev (OUT) The final document revision. Callers must free the memory
 * @param doc     (OUT) Callers are responsible for freeing the memory
 * @param n       (OUT) Number of bytes for doc
 * @param has_conflicts (OUT) Are there conflicts present for this document?
 */
int u1db_get_doc(u1database *db, const char *doc_id, char **doc_rev,
                 char **doc, int *n, int *has_conflicts);

/**
 * Internal API, Get the global database rev. If a negative number is returned,
 * an error occured.
 */
int u1db__get_db_rev(u1database *db);

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


#endif // _U1DB_H_
