.. _conflicts:

Conflicts, syncing, and revisions
========================================


Conflicts
-------------

If two u1dbs are synced, and then the same document is changed in different ways
in each u1db, and then they are synced again, there will be a *conflict*. This
does not block synchronisation: the document is registered as being in conflict,
and resolving that is up to the u1db-using application.

Importantly, **conflicts are not synced**. If *machine A* initiates a sync with
*machine B*, and this sync results in a conflict, the conflict **only registers
on machine A**. This policy is sometimes called "other wins": the machine you
synced *to* wins conflicts, and the document will have machine B's content on
both machine A and machine B. However, on machine A the document is marked
as having conflicts, and must be resolved there:

.. testsetup ::

    import u1db, json
    db=u1db.open(':memory:', True)
    docFromA=u1db.Document('test','machineA:1',json.dumps({'camefrom':'machineA'}))
    db.put_doc_if_newer(docFromA, save_conflict=True)
    docFromB=u1db.Document('test','machineB:1',json.dumps({'camefrom':'machineB'}))
    db.put_doc_if_newer(docFromB, save_conflict=True)

.. doctest ::

    >>> docFromB
    Document(test, machineB:1, conflicted, '{"camefrom": "machineB"}')
    >>> docFromB.has_conflicts # the document is in conflict
    True
    >>> conflicts = db.get_doc_conflicts(docFromB.doc_id)
    >>> print conflicts
    [(u'machineB:1', u'{"camefrom": "machineB"}'), (u'machineA:1', u'{"camefrom": "machineA"}')]
    >>> db.resolve_doc(docFromB, [x[0] for x in conflicts]) # resolve in favour of B
    >>> doc_is_now = db.get_doc("test")
    >>> doc_is_now.content # the content has been updated to doc's content
    u'{"camefrom": "machineB"}'
    >>> doc_is_now.has_conflicts # and is no longer in conflict
    False

Revisions
----------

As an app developer, you should treat a ``Document``'s ``revision`` as an opaque
cookie; do not try and deconstruct it or edit it. It is for your u1db 
implementation's use. You can therefore ignore the rest of this section.

If you are writing a new u1db implementation, understanding revisions is 
important, and this is where you find out about them.

(not yet written)

Syncing
-------
