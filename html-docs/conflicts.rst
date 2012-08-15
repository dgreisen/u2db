.. _conflicts:

Conflicts, Synchronisation, and Revisions
#########################################


Conflicts
---------

If two u1dbs are synced, and then the same document is changed in different
ways in each u1db, and then they are synced again, there will be a *conflict*.
This does not block synchronisation: the document is registered as being in
conflict, and resolving that is up to the u1db-using application.

Importantly, **conflicts are not synced**. If *machine A* initiates a sync with
*machine B*, and this sync results in a conflict, the conflict **only registers
on machine A**. This policy is sometimes called "other wins": the machine you
synced *to* wins conflicts, and the document will have machine B's content on
both machine A and machine B. However, on machine A the document is marked as
having conflicts, and must be resolved there:

.. testsetup ::

    import u1db, json
    db=u1db.open(':memory:', True)
    docFromA=u1db.Document('test','machineA:1',json.dumps({'camefrom':'machineA'}))
    db._put_doc_if_newer(docFromA, save_conflict=True, replica_uid='machineA', replica_gen=1)
    docFromB=u1db.Document('test','machineB:1',json.dumps({'camefrom':'machineB'}))
    db._put_doc_if_newer(docFromB, save_conflict=True, replica_uid='machineB', replica_gen=1)

.. doctest ::

    >>> docFromB
    Document(test, machineB:1, conflicted, '{"camefrom": "machineB"}')
    >>> docFromB.has_conflicts # the document is in conflict
    True
    >>> conflicts = db.get_doc_conflicts(docFromB.doc_id)
    >>> conflicts
    [Document(test, machineB:1, conflicted, u'{"camefrom": "machineB"}'), Document(test, machineA:1, u'{"camefrom": "machineA"}')]
    >>> db.resolve_doc(docFromB, [d.rev for d in conflicts]) # resolve in favour of B
    >>> doc_is_now = db.get_doc("test")
    >>> doc_is_now.content # the content has been updated to doc's content
    {u'camefrom': u'machineB'}
    >>> db.get_doc_conflicts(docFromB.doc_id)
    []
    >>> doc_is_now.has_conflicts # and is no longer in conflict
    False

Note that ``put_doc`` will fail because we got conflicts from a sync, but it
may also fail for another reason. If you acquire a document before a sync and
then sync, and the sync updates that document, then re-putting that document
with modified content will also fail, because the revision is not the current
one. This will raise a ``RevisionConflict`` error.

Synchronisation
---------------

Synchronisation between two u1db replicas consists of the following steps:

    1. The source replica asks the target replica for the information it has
       stored about the last time these two replicas were synchronised (If
       ever.)

    2. The source replica validates that its information regarding the last
       synchronisation is consistent with the target's information, and
       raises an error if not. (This could happen for instance if one of the
       replicas was lost and restored from backup, or if a user inadvertently
       tries to synchronise a copied database.)

    3. The source replica generates a list of changes since the last change the
       target replica knows of.

    4. The source replica checks what the last change is it knows about on the
       target replica.

    5. If there have been no changes on either replica that the other side has
       not seen, the synchronisation stops here.

    6. The source replica sends the changed documents to the target, along with
       what the latest change is that it knows about on the target replica.

    7. The target processes the changed documents, and records the source
       replica's latest change.

    8. The target responds with the documents that have changes that the source
       does not yet know about.

    9. The source processes the changed documents, and records the target
       replica's latest change.

    10. If the source has seen no changes unrelated to the synchronisation
        during this whole process, it now sends the target what its latest
        change is, so that the next synchronisation does not have to consider
        changes that were the result of this one.

The synchronisation information stored by the replica for each other replica it
has ever synchronised with consists of:

    * The replica id of the other replica. (Which should be globally unique
      identifier to distinguish database replicas from one another.)
    * The last known generation and transaction id of the other replica.
    * The generation and transaction id of *this* replica at the time of the
      most recent succesfully completed synchronisation with the other replica.

The generation is a counter that increases with each change to the database.
The transaction id is a unique random string that is paired with a particular
generation to identify cases where one of the replicas has been copied or
reverted to an earlier state by a restore from backup, and then diverged from
the known state on the other side of the synchronisation.

Implementations are not required to use transaction ids. If they don't they
should return an empty string when asked for a transaction id. All
implementations should accept an empty string as a valid transaction id.

Revisions
---------

As an app developer, you should treat a ``Document``'s ``revision`` as an
opaque cookie; do not try and deconstruct it or edit it. It is for your u1db
implementation's use. You can therefore ignore the rest of this section.

If you are writing a new u1db implementation, understanding revisions is
important, and this is where you find out about them.

To keep track of document revisions u1db uses vector versions. Each
synchronised instance of the same database is called a replica and has a unique
identifier (``replica uid``) assigned to it (currently the reference
implementation by default uses UUID4s for that); a revision is a mapping
between ``replica uids`` and ``generations``, as follows: ``rev
= <replica_uid:generation...>``, or using a functional notation
``rev(replica_uid) = generation``. The current concrete format is a string
built out of each ``replica_uid`` concatenated with ``':'`` and with its
generation in decimal, sorted lexicographically by ``replica_uid`` and then all
joined with ``'|'``, for example: ``'replicaA:1|replicaB:3'`` . Absent
``replica uids`` in a revision mapping are implicitly mapped to generation 0.

The new revision of a document modified locally in a replica, is the
modification of the old revision where the generation mapped to the editing
``replica uid`` is increased by 1.

When syncing one needs to establish whether an incoming revision is newer than
the current one or in conflict. A revision

``rev1 = <replica_1i:generation1i|i=1..n>``

is newer than a different

``rev2 = <replica_2j:generation2j|j=1..m>``

if for all ``i=1..n``, ``rev2(replica_1i) <= generation1i``

and for all ``j=1..m``, ``rev1(replica_2j) >= generation2j``.

Two revisions which are not equal nor one newer than the other are in conflict.

When resolving a conflict locally in a replica ``replica_resol``, starting from
``rev1...revN`` in conflict, the resulting revision ``rev_resol`` is obtained
by:

     ``R`` is the set the of all replicas explicitly mentioned in ``rev1..revN``

     ``rev_resol(r) = max(rev1(r)...revN(r))`` for all ``r`` in ``R``, with ``r != rev_resol``

     ``rev_resol(replica_resol) = max(rev1(replica_resol)...revN(replica_resol))+1``
