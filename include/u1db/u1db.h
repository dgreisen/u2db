/*
 * Copyright 2011-2012 Canonical Ltd.
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

#ifndef _U1DB_H_
#define _U1DB_H_

typedef struct _u1database u1database;
// The document structure. Note that you must use u1db_make_doc to create
// these, as there are private attributes that are required. This is just the
// public interface
typedef struct _u1db_document
{
    char *doc_id;
    size_t doc_id_len;
    char *doc_rev;
    size_t doc_rev_len;
    char *content;
    size_t content_len;
    int has_conflicts;
} u1db_document;


#define U1DB_OK 0
#define U1DB_INVALID_PARAMETER -1
// put_doc() was called but the doc_rev stored in the database doesn't match
// the one supplied.
#define U1DB_REVISION_CONFLICT -2
#define U1DB_INVALID_DOC_ID -3
#define U1DB_DOCUMENT_ALREADY_DELETED -4
#define U1DB_DOCUMENT_DOES_NOT_EXIST -5
#define U1DB_NOMEM -6

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
 * @param content: The JSON string representing the document. The content will
 *                 be copied and managed by the 'doc' parameter.
 * @param doc_id: A string identifying the document. If the value supplied is
 *                NULL, then a new doc_id will be generated.
 * @param doc: (OUT) a u1db_document that will be allocated and needs to be
 *             freed with u1db_free_doc
 * @return a status code indicating success or failure.
 */
int u1db_create_doc(u1database *db, const char *content, const char *doc_id,
                    u1db_document **doc);

/**
 * Put new document content for the given document identifier.
 *
 * @param doc: (IN/OUT) A document whose content we want to update in the
 *             database. The new content should have been set with
 *             u1db_doc_set_content. The document's revision should match what
 *             is currently in the database, and will be updated to point at
 *             the new revision.
 */
int u1db_put_doc(u1database *db, u1db_document *doc);

/**
 * Get the document defined by the given document id.
 *
 * @param doc_id: The document we are looking for
 * @param doc: (OUT) a document (or NULL) matching the request
 * @return status, will be U1DB_OK if there is no error, even if there is no
 *      document matching that doc_id.
 */
int u1db_get_doc(u1database *db, const char *doc_id, u1db_document **doc);

/**
 * Mark a document as deleted.
 *
 * @param doc (IN/OUT) The document we want to delete, the document must match
 *                the stored value, or the delete will fail. After being
 *                deleted, the doc_rev parameter will be updated to match the
 *                new value in the database.
 */
int u1db_delete_doc(u1database *db, u1db_document *doc);

/**
 * Get the document defined by the given document id.
 *
 * @param db_rev The global database revision to start at. You can pass '0' to
 *               get all changes in the database. The integer will be updated
 *               to point at the current db_rev.
 * @param cb     A callback function. This will be called passing in 'context',
 *               and a document identifier for each document that has been modified.
 *               The doc_id string is transient, so callers must copy it to
 *               their own memory if they want to keep it.
 *               If a document is changed more than once, it is currently
 *               undefined whether this will call cb() once per change, or just
 *               once per doc_id.
 * @param context Opaque context, passed back to the caller.
 */
int u1db_whats_changed(u1database *db, int *db_rev,
                       int (*cb)(void *, char *doc_id), void *context);


/**
 * Free a u1db_document.
 *
 * @param doc: A reference to the doc pointer to be freed. Generally used as:
 *             u1db_free_doc(&doc). If the pointer or its referenced value is
 *             NULL, this is a no-op. We will set the reference to NULL after
 *             freeing the memory.
 */
void u1db_free_doc(u1db_document **doc);

/**
 * Set the content for the document.
 *
 * This will copy the string, since the memory is managed by the doc object
 * itself.
 */
int u1db_doc_set_content(u1db_document *doc, const char *content);
#endif // _U1DB_H_
