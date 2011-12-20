.. _quickstart:

Quickstart
========================================

How to start working with the u1db Python implementation.

Getting u1db
------------

Download
^^^^^^^^

This is the recommended version of u1db to use for your Python application.

**Current release**: (link to tarball)

Use from source control
^^^^^^^^^^^^^^^^^^^^^^^

u1db is `maintained in bazaar in Launchpad <http://launchpad.net/u1db/>`_. To fetch the latest version,
`bzr branch lp:u1db`.

Starting u1db
-------------

.. doctest ::

    >>> import u1db, json, tempfile
    >>> db = u1db.open(":memory:", create=True)
    
    >>> content = json.dumps({"name": "Alan Hansen"}) # create a document
    >>> doc = db.create_doc(content)
    >>> print doc.content
    {"name": "Alan Hansen"}
    >>> doc.content = json.dumps({"name": "Alan Hansen", "position": "defence"}) # update the document's content
    >>> rev = db.put_doc(doc)
    
    >>> content = json.dumps({"name": "John Barnes", "position": "forward"}) # create more documents
    >>> doc2 = db.create_doc(content)
    >>> content = json.dumps({"name": "Ian Rush", "position": "forward"})
    >>> doc2 = db.create_doc(content)
    
    >>> db.create_index("by-position", ("position",)) # create an index by passing an index expression
    
    >>> results = db.get_from_index("by-position", [("forward",)]) # query that index by passing a list of tuples of queries
    >>> len(results)
    2
    >>> data = [json.loads(result.content) for result in results]
    >>> names = [item["name"] for item in data]
    >>> sorted(names)
    [u'Ian Rush', u'John Barnes']
    

