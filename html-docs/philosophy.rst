.. _philosophy:

Philosophy
========================================

Some notes on what u1db is for, how it works, and how it should be used.

U1DB is a cross-platform, cross-device, syncable database. In order to be this
way, there's a philosophy behind it. Key to this philosophy is that u1db can
be implemented in many languages and on top of many back ends: this means that
the API needs to be, as much as possible, portable between very different
languages. Each implementation should implement :ref:`high-level-api` in the
way appropriate to that language (Python uses tuples all over the place,
Vala/C use a Document object for most things, and so on), but it's important
that an implementation not diverge from the API. Because u1db is a syncable
database, it's quite likely that an app developer using it will be building their
app on multiple platforms at once. Knowledge that an app developer has from
having built a u1db app on one platform should be transferable to another
platform. This means that querying is the same across platforms; storing and
retrieving docs is the same across platforms; syncing is the same across
platforms. U1DB is also syncable to Ubuntu One, which is a very large 
server installation; the API needs to be suitable to run at scales from a
mobile phone up to a large server installation.

To this end, there are a few guidelines. Primarily, the guideline the u1db team
used for the largest u1db is somewhere around 10,000 documents. It's important
to note that this is not an *enforced* limit; an app dev can store a zillion
documents in a u1db if they want. However, the implementations are allowed to
assume that there aren't a zillion documents; in particular, suggestions for
API changes which make things more annoying for a 1,000 documents use-case
in order to help with a zillion documents are not likely to be adopted.

Similarly, suggested changes to the high-level API which are very difficult to
implement in static languages like C are also unlikely to be adopted, in order
to maintain the goal of knowledge on one platform transferring to another.

It should be easy to sync a u1db from place to place. There is a direct server
HTTP API, which allows an app to work with a u1db on the server without any
locally-stored data. However, this is not the preferred way to work; to edit
data in a u1db, the easiest course should be to sync that database, edit it,
and then sync it back. If this is not the easiest course, then that's a bug.

