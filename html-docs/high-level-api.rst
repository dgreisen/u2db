.. _high-level-api:

The high-level API
##################

The U1DB API has three separate sections: document storage and retrieval,
querying, and sync. Here we describe the high-level API. Remember that you will
need to choose an implementation, and exactly how this API is defined is
implementation-specific, in order that it fits with the language's conventions.

Document storage and retrieval
------------------------------

U1DB stores documents. A document is a set of nested key-values; basically,
anything you can express with JSON. Implementations are likely to provide
a Document object "wrapper" for these documents; exactly how the wrapper works
is implementation-defined.

Creating documents
^^^^^^^^^^^^^^^^^^

To create a document, use :py:meth:`~u1db.Database.create_doc` or
:py:meth:`~u1db.Database.create_doc_from_json`. Code examples below are from
:ref:`reference-implementation` in Python. :py:meth:`~u1db.Database.create_doc`
takes a dictionary-like object, and
:py:meth:`~u1db.Database.create_doc_from_json` a JSON string.

.. testsetup ::

    import os, tempfile
    old_dir = os.path.realpath('.')
    tmp_dir = tempfile.mkdtemp()
    os.chdir(tmp_dir)

.. doctest ::

    >>> import u1db
    >>> db = u1db.open("mydb1.u1db", create=True)
    >>> doc = db.create_doc({"key": "value"}, doc_id="testdoc")
    >>> doc.content
    {'key': 'value'}
    >>> doc.doc_id
    'testdoc'


Retrieving documents
^^^^^^^^^^^^^^^^^^^^

The simplest way to retrieve documents from a u1db is by calling
:py:meth:`~u1db.Database.get_doc` with a ``doc_id``. This will return a
:py:class:`~u1db.Document` object [#]_.

.. doctest ::

    >>> import u1db
    >>> db = u1db.open("mydb4.u1db", create=True)
    >>> doc = db.create_doc({"key": "value"}, doc_id="testdoc")
    >>> doc1 = db.get_doc("testdoc")
    >>> doc1.content
    {u'key': u'value'}
    >>> doc1.doc_id
    'testdoc'

And it's also possible to retrieve many documents by ``doc_id``.

.. doctest ::

    >>> import u1db
    >>> db = u1db.open("mydb5.u1db", create=True)
    >>> doc1 = db.create_doc({"key": "value"}, doc_id="testdoc1")
    >>> doc2 = db.create_doc({"key": "value"}, doc_id="testdoc2")
    >>> for doc in db.get_docs(["testdoc2","testdoc1"]):
    ...     print doc.doc_id
    testdoc2
    testdoc1

Note that :py:meth:`u1db.Database.get_docs` returns the documents in the order
specified.

Editing existing documents
^^^^^^^^^^^^^^^^^^^^^^^^^^

Editing an *existing* document is done with ``put_doc()``. This is separate
from ``create_doc()`` so as to avoid accidental overwrites. ``put_doc()`` takes
a ``Document`` object, because the object encapsulates revision information for
a particular document. This revision information must match what is stored in
the database, so we can make sure you are not overwriting another version
of the document that you dont know about (eg, new documents that came from
a background sync while you were editing your copy).

.. doctest ::

    >>> import u1db
    >>> db = u1db.open("mydb2.u1db", create=True)
    >>> doc1 = db.create_doc({"key1": "value1"}, doc_id="doc1")

    >>> # the next line should fail because it's creating a doc that already exists
    >>> db.create_doc({"key1fail": "value1fail"}, doc_id="doc1")
    Traceback (most recent call last):
        ...
    RevisionConflict

    >>> # Now editing the doc with the doc object we got back...
    >>> doc1.content["key1"] = "edited"
    >>> db.put_doc(doc1) # doctest: +ELLIPSIS
    '...'
    >>> doc2 = db.get_doc(doc1.doc_id)
    >>> doc2.content
    {u'key1': u'edited'}


Finally, deleting a document is done with :py:meth:`~u1db.Database.delete_doc`.

.. doctest ::

    >>> import u1db
    >>> db = u1db.open("mydb3.u1db", create=True)
    >>> doc = db.create_doc({"key": "value"})
    >>> db.delete_doc(doc) # doctest: +ELLIPSIS
    '...'
    >>> db.get_doc(doc.doc_id)
    >>> doc = db.get_doc(doc.doc_id, include_deleted=True)
    >>> doc.content

Document functions
^^^^^^^^^^^^^^^^^^

* :py:meth:`~u1db.Database.create_doc`
* :py:meth:`~u1db.Database.create_doc_from_json`
* :py:meth:`~u1db.Database.put_doc`
* :py:meth:`~u1db.Database.get_doc`
* :py:meth:`~u1db.Database.get_docs`
* :py:meth:`~u1db.Database.get_all_docs`
* :py:meth:`~u1db.Database.delete_doc`
* :py:meth:`~u1db.Database.whats_changed`

Querying
--------

To retrieve documents other than by ``doc_id``, you query the database.
Querying a U1DB is done by means of an index. To retrieve only some documents
from the database based on certain criteria, you must first create an index,
and then query that index.

An index is created from ''index expressions''. An index expression names one
or more fields in the document. A simple example follows: view many more
examples here.

Given a database with the following documents:

.. doctest ::

    >>> import u1db
    >>> db1 = u1db.open("mydb6.u1db", create=True)
    >>> jb = db1.create_doc({"firstname": "John", "surname": "Barnes", "position": "left wing"})
    >>> jm = db1.create_doc({"firstname": "Jan", "surname": "Molby", "position": "midfield"})
    >>> ah = db1.create_doc({"firstname": "Alan", "surname": "Hansen", "position": "defence"})
    >>> jw = db1.create_doc({"firstname": "John", "surname": "Wayne", "position": "filmstar"})

an index expression of ``"firstname"`` will create an index that looks
(conceptually) like this

====================== ========
index expression value document
====================== ========
Alan                   ah
Jan                    jm
John                   jb
John                   jw
====================== ========

and that index is created with:

.. doctest ::

    >>> db1.create_index("by-firstname", "firstname")
    >>> sorted(db1.get_index_keys('by-firstname'))
    [(u'Alan',), (u'Jan',), (u'John',)]

-- that is, create an index with a name and one or more index expressions.
(Exactly how to pass the name and the list of index expressions is something
specific to each implementation.)

Index expressions
^^^^^^^^^^^^^^^^^

An index expression describes how to get data from a document; you can think of
it as describing a function which, when given a document, returns a value,
which is then used as the index key.

**Name a field.** A basic index expression is a dot-delimited list of nesting
fieldnames, so the index expression ``field.sub1.sub2`` applied to a document
with below content:

.. doctest ::

    >>> import u1db
    >>> db = u1db.open('mydb7.u1db', create=True)
    >>> db.create_index('by-subfield', 'field.sub1.sub2')
    >>> doc1 = db.create_doc({"field": {"sub1": {"sub2": "hello", "sub3": "not selected"}}})
    >>> db.get_index_keys('by-subfield')
    [(u'hello',)]

gives the index key "hello", and therefore an entry in the index of

========= ====
Index key doc
========= ====
hello     doc1
========= ====

**Name a list.** If an index expression names a field whose contents is a list
of strings, the document will have multiple entries in the index, one per entry
in the list. So, the index expression ``field.tags`` applied to a document with
content:

.. doctest ::

    >>> import u1db
    >>> db = u1db.open('mydb8.u1db', create=True)
    >>> db.create_index('by-tags', 'field.tags')
    >>> doc2 = db.create_doc({"field": {"tags": [ "tag1", "tag2", "tag3" ]}})
    >>> sorted(db.get_index_keys('by-tags'))
    [(u'tag1',), (u'tag2',), (u'tag3',)]

gives index entries

========= ====
Index key doc
========= ====
tag1      doc2
tag2      doc2
tag3      doc2
========= ====

**Subfields of objects in a list.** If an index expression points at subfields
of objects in a list, the document will have multiple entries in the index, one
for each object in the list that specifies the denoted subfield. For instance
the index expression ``managers.phone_number`` applied to a document
with content:

.. doctest ::

    >>> import u1db
    >>> db = u1db.open('mydb9.u1db', create=True)
    >>> db.create_index('by-phone-number', 'managers.phone_number')
    >>> doc3 = db.create_doc(
    ...    {"department": "department of redundancy department",
    ...    "managers": [
    ...        {"name": "Mary", "phone_number": "12345"},
    ...        {"name": "Katherine"},
    ...        {"name": "Rob", "phone_number": "54321"}]})
    >>> sorted(db.get_index_keys('by-phone-number'))
    [(u'12345',), (u'54321',)]


would give index entries:

========= ====
Index key doc
========= ====
12345     doc3
54321     doc3
========= ====

**Transformation functions.** An index expression may be wrapped in any number
of transformation functions. A function transforms the result of the contained
index expression: for example, if an expression ``name.firstname`` generates
"John" when applied to a document, then ``lower(name.firstname)`` generates
"john".

Available transformation functions are:

* ``lower(index_expression)`` - lowercase the value
* ``split_words(index_expression)`` - split the value on whitespace; will act
  like a list and add multiple entries to the index
* ``number(index_expression, width)`` - takes an integer value, and turns it
  into a string, left padded with zeroes, to make it at least as wide as
  width; or nothing if the field type is not an integer.
* ``bool(index_expression)`` - takes a boolean value and turns it into '0' if
  false and '1' if true, or nothing if the field type is not boolean.
* ``combine(index_expression1, index_expression2, ...)`` - Combine the values
  of an arbitrary number of sub expressions into a single index.

So, the index expression ``splitwords(lower(field.name))`` applied to
a document with content:

.. doctest ::

    >>> import u1db
    >>> db = u1db.open('mydb10.u1db', create=True)
    >>> db.create_index('by-split-lower', 'split_words(lower(field.name))')
    >>> doc4 = db.create_doc({"field": {"name": "Bruce David Grobbelaar"}})
    >>> sorted(db.get_index_keys('by-split-lower'))
    [(u'bruce',), (u'david',), (u'grobbelaar',)]

gives index entries

========== ====
Index key  doc
========== ====
bruce      doc3
david      doc3
grobbelaar doc3
========== ====


Querying an index
^^^^^^^^^^^^^^^^^

Pass an index key or multiple index keys (if the index is on multiple fields)
to ``get_from_index``; the last index key (and *only* the last
one) can end with an asterisk, which matches initial substrings. So, querying
our ``by-firstname`` index from above:

.. doctest ::

    >>> johns = [d.doc_id for d in db1.get_from_index("by-firstname", "John")]
    >>> assert(jw.doc_id in johns)
    >>> assert(jb.doc_id in johns)
    >>> assert(jm.doc_id not in johns)

will return the documents with ids: 'jw', 'jb'.

``get_from_index("by_firstname", "J*")`` will match all index keys beginning
with "J", and so will return the documents with ids: 'jw', 'jb', 'jm'.

.. doctest ::

    >>> js = [d.doc_id for d in db1.get_from_index("by-firstname", "J*")]
    >>> assert(jw.doc_id in js)
    >>> assert(jb.doc_id in js)
    >>> assert(jm.doc_id in js)

Index key values used when querying are always strings. If querying an
index built with ``bool()`` use the values '0' and '1' to query for
false and true respectively. For an index built with ``number()`` turn
key number values into strings with the appropriate left zero-padding
(and note in particular that querying a ``number()`` index with an int
value, such as ``db.get_from_index('by-number', 9)``, will throw an error).

.. doctest ::

    >>> db = u1db.open('mydb11.u1db', create=True)
    >>> db.create_index('done', "bool(done)")
    >>> doc1 = db.create_doc({'task': 'milk', 'done': False, 'hours': 1})
    >>> doc2 = db.create_doc({'task': 'coding', 'done': True, 'hours': 12})
    >>> [doc.content['task'] for doc in db.get_from_index('done', '1')]
    [u'coding']
    >>> db.create_index('by-hours', "number(hours, 3)")
    >>> [doc.content['task'] for doc in db.get_from_index('by-hours', '12')]
    []
    >>> [doc.content['task'] for doc in db.get_from_index('by-hours', '012')]
    [u'coding']
    >>> [doc.content['task'] for doc in db.get_from_index('by-hours', '001')]
    [u'milk']


Index functions
^^^^^^^^^^^^^^^

* :py:meth:`~u1db.Database.create_index`
* :py:meth:`~u1db.Database.delete_index`
* :py:meth:`~u1db.Database.get_from_index`
* :py:meth:`~u1db.Database.get_range_from_index`
* :py:meth:`~u1db.Database.get_index_keys`
* :py:meth:`~u1db.Database.list_indexes`

Synchronising
-------------

U1DB is a syncable database. Any U1DB can be synced with any U1DB server; most
U1DB implementations are capable of being run as a server. Synchronising brings
both the server and the client up to date with one another; save data into a
local U1DB whether online or offline, and then sync when online.

Pass an HTTP URL to sync with that server.

Synchronising databases which have been independently changed may produce
conflicts.  Read about the U1DB conflict policy and more about synchronising at
:ref:`conflicts`.

Running your own U1DB server is implementation-specific.
:ref:`reference-implementation` is able to be run as a server.

Dealing with conflicts
----------------------

Synchronising a database can result in conflicts; if your user changes the same
document in two different places and then syncs again, that document will be
''in conflict'', meaning that it has incompatible changes. If this is the case,
:py:attr:`~u1db.Document.has_conflicts` will be true, and put_doc to a
conflicted doc will give a ``ConflictedDoc`` error. To get a list of conflicted
versions of the document, do :py:meth:`~u1db.Database.get_doc_conflicts`.
Deciding what the final unconflicted document should look like is obviously
specific to the user's application; once decided, call
:py:meth:`~u1db.Database.resolve_doc` to resolve and set the final resolved
content.

Synchronising Functions
^^^^^^^^^^^^^^^^^^^^^^^

* :py:meth:`~u1db.Database.sync`
* :py:meth:`~u1db.Database.get_doc_conflicts`
* :py:meth:`~u1db.Database.resolve_doc`

.. rubric:: footnotes

.. [#] Alternatively if a factory function was passed into
    :py:func:`u1db.open`, :py:meth:`~u1db.Database.get_doc` will return
    whatever type of object the factory function returns.

.. testcleanup ::

    os.chdir(old_dir)
    os.remove(os.path.join(tmp_dir, "mydb1.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb2.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb3.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb4.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb5.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb6.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb7.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb8.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb9.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb10.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb11.u1db"))
    os.rmdir(tmp_dir)
