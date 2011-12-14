.. u1db documentation master file, created by
   sphinx-quickstart on Tue Dec 13 13:22:21 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

U1DB
====

.. toctree::
   :maxdepth: 2

U1DB is a database API for synchronised databases of JSON documents. It's 
simple to use in applications, and allows apps to store documents and 
synchronise them between machines and devices. U1DB itself is not a database: 
instead, it's an API which can use any database as storage. This means that you 
can use u1db on different platforms, from different languages, and backed 
on to different databases, and sync between all of them.

The API for U1DB looks similar across all different implementations. This API
is described below. To actually use U1DB you'll need an implementation; a 
version of U1DB made available on your choice of platform, in your choice of
language, and on your choice of backend database.

If you're interested in using U1DB in an application, look at this API first,
and then choose an implementation and read about exactly how the U1DB API is
made available in that implementation.

If you're interested in hacking on U1DB itself, read about the rules for U1DB
and the reference implementation.

The high-level API
==================

The U1DB API has three separate sections: document storage and retrieval,
querying, and sync. Here we describe the high-level API. Remember that you
will need to choose an implementation, and exactly how this API is defined
is implementation-specific, in order that it fits with the language's 
conventions.

Document storage and retrieval
------------------------------

U1DB stores documents. A document is a set of nested key-values; basically,
anything you can express with JSON. Implementations are likely to provide a 
Document object "wrapper" for these documents; exactly how the wrapper works
is implementation-defined.

Creating and editing documents
==============================

To create a document, use ``create_doc(

Retrieving documents
====================

get_docs returns in order specified.

 * create_doc(JSON string, optional_doc_id)
 * put_doc(Document)
 * get_doc(doc_id)
 * get_docs(list_of_doc_ids)
 * delete_doc(Document)
 * whats_changed(generation)

Querying
--------

Querying a U1DB is done by means of an index. To retrieve only some documents
from the database based on certain criteria, you must first create an index,
and then query that index.

An index is created from ''index expressions''. An index expression names one
or more fields in the document. A simple example follows: view many more
examples here.

Given a database with the following documents::

    {"firstname": "John", "surname", "Barnes", "position": "left wing"} ID jb
    {"firstname": "Jan", "surname", "Molby", "position": "midfield"} ID jm
    {"firstname": "Alan", "surname", "Hansen", "position": "defence"} ID ah
    {"firstname": "John", "surname", "Wayne", "position": "filmstar"} ID jw

an index expression of ``["firstname"]`` will create an index that looks 
(conceptually) like this

====================== ===========
index expression value document id
====================== ===========
Alan                   ah
Jan                    jm
John                   jb
John                   jw
====================== ===========

and that index is created with ``create_index("by-firstname", ["firstname"])`` - that is,
create an index with a name and a list of index expressions. (Exactly how to
pass the name and the list of index expressions is something specific to
each implementation.)

Index expressions
^^^^^^^^^^^^^^^^^

An index expression describes how to get data from a document; you can think
of it as describing a function which, when given a document, returns a value,
which is then used as the index key.

**Name a field.** A basic index expression is a dot-delimited list of nesting
fieldnames, so the index expression ``field.sub1.sub2`` applied to a document 
with ID ``doc1`` and content::

  {
      "field": { 
          "sub1": { 
              "sub2": "hello"
              "sub3": "not selected"
          }
      }
  }

gives the index key "hello", and therefore an entry in the index of

========= ======
Index key doc_id
========= ======
hello     doc1
========= ======

**Name a list.** If an index expression names a field whose contents is a list
of strings, the doc will have multiple entries in the index, one per entry in
the list. So, the index expression ``field.tags`` applied to a document with 
ID "doc2" and content::

  {
      "field": { 
          "tags": [ "tag1", "tag2", "tag3" ]
      }
  }

gives index entries

========= ======
Index key doc_id
========= ======
tag1      doc2
tag2      doc2
tag3      doc2
========= ======

**Transformation functions.** An index expression may be wrapped in any number of
transformation functions. A function transforms the result of the contained
index expression: for example, if an expression ``name.firstname`` generates 
"John" when applied to a document, then ``lower(name.firstname)`` generates 
"john".

Available transformation functions are:

 * ``lower(index_expression)`` - lowercase the value
 * ``splitwords(index_expression)`` - split the value on whitespace; will act like a 
   list and add multiple entries to the index
 * ``is_null(index_expression)`` - True if value is null or not a string or the field 
   is absent, otherwise false

So, the index expression ``splitwords(lower(field.name))`` applied to a document with 
ID "doc3" and content::

  {
      "field": { 
          "name": "Bruce David Grobbelaar"
      }
  }

gives index entries

========== ======
Index key  doc_id
========== ======
bruce      doc3
david      doc3
grobbelaar doc3
========== ======


Querying an index
-----------------

Pass a list of tuples of index keys to ``get_from_index``; the last index key in
each tuple (and *only* the last one) can end with an asterisk, which matches 
initial substrings. So, querying our ``by-firstname`` index from above::

    get_from_index(
        "by-firstname",                     # name of index
            [                               # begin the list of index keys
                ("John", )                  # an index key
            ]                               # end the list
    )


will return ``[ 'jw', 'jb' ]`` - that is, a list of document IDs.

``get_from_index("by_firstname", [("J*")])`` will match all index keys beginning
with "J", and so will return ``[ 'jw', 'jb', 'jm' ]``.

``get_from_index("by_firstname", [("Jan"), ("Alan")])`` will match both the
queried index keys, and so will return ``[ 'jm', 'ah' ]``.


Index functions
^^^^^^^^^^^^^^^

 * create_index(name, index_expressions_list)
 * delete_index(name)
 * get_from_index(name, list_of_index_key_tuples)
 * get_keys_from_index(name)
 * list_indexes()

Syncing
-------

U1DB is a syncable database. Any U1DB can be synced with any U1DB server; most
U1DB implementations are capable of being run as a server. Syncing brings
both the server and the client up to date with one another; save data into a
local U1DB whether online or offline, and then sync when online.

 * sync(URL)
 * resolve_doc(self, Document, conflicted_doc_revs)

Pass an HTTP URL to sync with that server.

Syncing databases which have been independently changed may produce conflicts.
Read about the U1DB conflict policy and handling here.

Running your own U1DB server is implementation-specific. The U1DB reference
implementation is able to be run as a server.

Dealing with conflicts
^^^^^^^^^^^^^^^^^^^^^^

Syncing a database can result in conflicts; if your user changes the same 
document in two different places and then syncs again, that document will be
''in conflict'', meaning that it has incompatible changes. If this is the case,
doc.has_conflicts will be true, and put_doc to a conflicted doc will give a
ConflictedDoc error. To get a list of conflicted versions of the
document, do get_doc_conflicts(doc_id). Deciding what the final unconflicted
document should look like is obviously specific to the user's application; once
decided, call resolve_doc(doc, list_of_conflicted_revisions) to resolve, and
then put_doc as normal to set the final resolved content.

 * get_doc_conflicts(doc_id)
 * resolve_doc(doc, list_of_conflicted_revisions)

Implementations
===============

Choose the implementation you need and get hacking!

| Platform(s) | Language | Back end database ||
| Ubuntu, Windows, OS X | Python | SQLite | implementation |
| Ubuntu | Vala | SQLite | implementation |
| Web | JavaScript | localStorage | implementation |
| Android | Java | SQLite | implementation |
| iOS | Objective C | SQLite | implementation |



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

