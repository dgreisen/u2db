.. u1db documentation master file, created by
   sphinx-quickstart on Tue Dec 13 13:22:21 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

U1DB
====

U1DB is a database API for synchronised databases of JSON documents. It's 
simple to use in applications, and allows apps to store documents and 
synchronise them between machines and devices. U1DB itself is not a database: 
instead, it's an API which can use any database as storage. This means that you 
can use u1db on different platforms, from different languages, and backed 
on to different databases, and sync between all of them.

The API for U1DB looks similar across all different implementations. This API
is described at :ref:`high-level-api`. To actually use U1DB you'll need an 
implementation; a version of U1DB made available on your choice of platform, 
in your choice of language, and on your choice of backend database.

If you're interested in using U1DB in an application, look at 
:ref:`high-level-api` first, and then choose one of the :ref:`implementations` 
and read about exactly how the U1DB API is made available in that 
implementation. Get going quickly with the :ref:`quickstart` guide.

If you're interested in hacking on U1DB itself, read about the rules for U1DB
and :ref:`reference-implementation`.

.. toctree::
   :maxdepth: 1
   
   quickstart
   high-level-api
   reference-implementation
   conflicts


.. _implementations:

Implementations
===============

Choose the implementation you need and get hacking!

===================== =========== ================= ==============
Platform(s)           Language    Back end database link
===================== =========== ================= ==============
Ubuntu, Windows, OS X Python      SQLite            :ref:`reference-implementation`
Ubuntu                Vala        SQLite            `lp:shardbridge <http://launchpad.net/shardbridge/>`_
Web                   JavaScript  localStorage      planned
Android               Java        SQLite            planned
iOS                   Objective C SQLite            planned
===================== =========== ================= ==============


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

