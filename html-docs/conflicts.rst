.. _conflicts:

Conflicts, Synchronization, and Revisions
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

    import os, tempfile
    old_dir = os.path.realpath('.')
    tmp_dir = tempfile.mkdtemp()
    os.chdir(tmp_dir)

.. doctest ::

    >>> import u1db, json
    >>> db1 = u1db.open('mydb1.u1db', create=True)
    >>> db2 = u1db.open('mydb2.u1db', create=True)
    >>> doc1 = db1.create_doc({'came_from':'replica_1'})
    >>> doc_id = doc1.doc_id
    >>> # create a document in database 2 with the same document id.
    >>> doc2 = db2.create_doc({'came_from':'replica_2'}, doc_id=doc_id)
    >>> sync_target = db1.get_sync_target()
    >>> synchronizer = u1db.sync.Synchronizer(db2, sync_target)
    >>> synchronizer.sync()  # returns the local generation before sync
    1
    >>> db1_doc = db1.get_doc(doc_id)
    >>> db1_doc.content
    {u'came_from': u'replica_1'}
    >>> conflicted_doc = db2.get_doc(doc_id)
    >>> conflicted_doc.content  # the sync target's content wins
    {u'came_from': u'replica_1'}
    >>> conflicted_doc.has_conflicts # but the document is in conflict
    True
    >>> conflicts = db2.get_doc_conflicts(doc_id)
    >>> len(conflicts)  # There are two conflicting versions of the document
    2
    >>> db2.resolve_doc(conflicts[1], [d.rev for d in conflicts]) # resolve in favour db2's version
    >>> doc_is_now = db2.get_doc(doc_id)
    >>> doc_is_now.content # the content has been updated to doc's content
    {u'came_from': u'replica_2'}
    >>> db2.get_doc_conflicts(doc_id)
    []
    >>> doc_is_now.has_conflicts # and is no longer in conflict
    False
    >>> # synchronize again so that the resolved version ends up in db1
    >>> synchronizer.sync(sync_target)
    3
    >>> db1_doc = db1.get_doc(doc_id)
    >>> db1_doc.content  # now is identical to db2's version
    {u'came_from': u'replica_2'}
    >>> db2_doc = db2.get_doc(doc_id)
    >>> db2_doc.has_conflicts  # this sync did not create any new conflicts
    False

.. testcleanup ::

    os.chdir(old_dir)
    os.remove(os.path.join(tmp_dir, "mydb1.u1db"))
    os.remove(os.path.join(tmp_dir, "mydb2.u1db"))
    os.rmdir(tmp_dir)

Note that ``put_doc`` will fail because we got conflicts from a sync, but it
may also fail for another reason. If you acquire a document before a sync and
then sync, and the sync updates that document, then re-putting that document
with modified content will also fail, because the revision is not the current
one. This will raise a ``RevisionConflict`` error.

Synchronization
---------------

Synchronization between two u1db replicas consists of the following steps:

    1. The source replica asks the target replica for the information it has
       stored about the last time these two replicas were synchronized (If
       ever.)

    2. The source replica validates that its information regarding the last
       synchronization is consistent with the target's information, and
       raises an error if not. (This could happen for instance if one of the
       replicas was lost and restored from backup, or if a user inadvertently
       tries to synchronize a copied database.)

    3. The source replica generates a list of changes since the last change the
       target replica knows of.

    4. The source replica checks what the last change is it knows about on the
       target replica.

    5. If there have been no changes on either replica that the other side has
       not seen, the synchronization stops here.

    6. The source replica sends the changed documents to the target, along with
       what the latest change is that it knows about on the target replica.

    7. The target processes the changed documents, and records the source
       replica's latest change.

    8. The target responds with the documents that have changes that the source
       does not yet know about.

    9. The source processes the changed documents, and records the target
       replica's latest change.

    10. If the source has seen no changes unrelated to the synchroniztion
        during this whole process, it now sends the target what its latest
        change is, so that the next synchronization does not have to consider
        changes that were the result of this one.

The synchronization information stored by the replica for each other replica it
has ever synchronized with consists of:

    * The replica id of the other replica. (Which should be globally unique
      identifier to distinguish database replicas from one another.)
    * The last known generation and transaction id of the other replica.
    * The generation and transaction id of *this* replica at the time of the
      most recent succesfully completed synchronization with the other replica.

The generation is a counter that increases with each change to the database.
The transaction id is a unique random string that is paired with a particular
generation to identify cases where one of the replicas has been copied or
reverted to an earlier state by a restore from backup, and then diverged from
the known state on the other side of the synchronization.

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
synchronized instance of the same database is called a replica and has a unique
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
