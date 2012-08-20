Tutorial
########

In this tutorial we will demonstrate what goes into creating an application
that uses u1db as a backend. We will use code samples from the simple todo list
application 'Cosas' as our example. The full source code to Cosas can be found
in the u1db source tree.  It comes with a user interface, but we will only
focus on the code that interacts with u1db here.

Defining the Task Object
------------------------

First we need to define what we'll actually store in u1db. For a todo list
application, it makes sense to have each todo item or task be a single
document in the database, so that we can use indexes to find individual tasks
with specific properties.

We'll subclass Document, and define some properties that we think our tasks
need to have. There are no schema's in u1db, which means we can always change
the structure of the underlying json document at a later time. (Though that
does likely mean we will have to migrate older documents for them to still work
with the new code.)

Let's give our Task objects a title, a (boolean) done property, and a list of
tags, so that the json representation of a task would look something like
this:

.. code-block:: python

    '{"title": "the task at hand",
      "done": false,
      "tags": ["urgent", "priority 1", "today"]}'

We can define ``Task`` as follows:

.. testcode ::

    import u1db

    class Task(u1db.Document):
        """A todo item."""

        def _get_title(self):
            """Get the task title."""
            return self.content.get('title')

        def _set_title(self, title):
            """Set the task title."""
            self.content['title'] = title

        title = property(_get_title, _set_title, doc="Title of the task.")

        def _get_done(self):
            """Get the status of the task."""
            return self.content.get('done', False)

        def _set_done(self, value):
            """Set the done status."""
            self.content['done'] = value

        done = property(_get_done, _set_done, doc="Done flag.")

        def _get_tags(self):
            """Get tags associated with the task."""
            return self.content.setdefault('tags', [])

        def _set_tags(self, tags):
            """Set tags associated with the task."""
            self.content['tags'] = list(set(tags))

        tags = property(_get_tags, _set_tags, doc="Task tags.")

As you can see, :py:class:`~u1db.Document` objects come with a .content
property, which is a Python dictionary. This is where we look up or store all
data pertaining to the task.

We can now create tasks, set their titles:

.. testcode ::

    example_task = Task()
    example_task.title = "Create a Task class."
    print(example_task.title)

.. testoutput ::

    Create a Task class.

their tags:

.. testcode ::

    print(example_task.tags)

.. testoutput ::

    []

.. testcode ::

    example_task.tags = ['develoment']
    print(example_task.tags)

.. testoutput ::

    ['develoment']

and their done status:

.. testcode ::

    print(example_task.done)

.. testoutput ::

    False

.. testcode ::

    example_task.done = True
    print(example_task.done)

.. testoutput ::

    True

This is all we need the task object to do: as long as we have a way to store
all its data in the .content dictionary, the super class will take care of
converting that into JSON so it can be stored in the database.

Defining Indexes
----------------

Now that we have tasks defined, we will probably want to query the database
using their properties. To that end, we will need to use indexes. Let's define
two for now, one to query by tags, and one to query by done status. We'll
define some global constants with the name and the definition of the indexes,
which will make them easier to refer to in the rest of the code:

.. code-block:: python

    TAGS_INDEX = 'tags'
    DONE_INDEX = 'done'
    INDEXES = {
        TAGS_INDEX: ['tags'],
        DONE_INDEX: ['bool(done)'],
    }

``INDEXES`` is just a regular dictionary, with the names of the indexes as
keys, and the index definitions, which are lists of expressions as values. (We
chose to use lists since an index can be defined on multiple fields, though
both of the indexes defined above only index a single field.)

The ``tags`` index will index any document that has a top level field ``tags``
and index its value. Our tasks will have a list value under ``tags`` which
means that u1db will index each task for each of the values in the list in this
index.

The ``done`` index will index any document that has a boolean value in a top
level field with the name ``done``.

We will see how the indexes are actually created and queried below.

Storing and Retrieving Tasks
----------------------------

To store and retrieve our task objects we'll need a u1db
:py:class:`~u1db.Database`. We can make a little helper function to get a
reference to our application's database, and create it if it doesn't already
exist:


.. code-block:: python

    from dirspec.basedir import save_data_path

    def get_database():
        """Get the path that the database is stored in."""
        return u1db.open(
            os.path.join(save_data_path("cosas"), "cosas.u1db"), create=True,
            document_factory=Task)

There are a few things to note here: First of all, we use
`lp:dirspec <http://launchpad.net/dirspec/>`_ to handle where to find or put
the database in a way that works across platforms. This is not something
specific to u1db, so you could choose to use it for your own application or
not: :py:func:`u1db.open` will happily take any filesystem path. Secondly, we
pass our Task class into the ``document_factory`` argument of
:py:func:`u1db.open`. This means that any time we get documents from the
database, it will return Task objects, so we don't have to do the conversion in
our code.

Now we create a TodoStore class that will handle all interactions with the
database:

.. code-block:: python

    class TodoStore(object):
        """The todo application backend."""

        def __init__(self, db):
            self.db = db

        def initialize_db(self):
            """Initialize the database."""
            # Ask the database for currently existing indexes.
            db_indexes = dict(self.db.list_indexes())
            # Loop through the indexes we expect to find.
            for name, expression in INDEXES.items():
                if name not in db_indexes:
                    # The index does not yet exist.
                    self.db.create_index(name, *expression)
                    continue
                if expression == db_indexes[name]:
                    # The index exists and is up to date.
                    continue
                # The index exists but the definition is not what expected, so we
                # delete it and add the proper index expression.
                self.db.delete_index(name)
                self.db.create_index(name, *expression)

The ``initialize_db()`` method checks whether the database already has the
indexes we defined above and if it doesn't or if the definition is different
than the one we have, the index is (re)created. We will call this method every
time we start the application, to make sure all the indexes are up to date.
Creating an index is a matter of calling :py:meth:`~u1db.Database.create_index`
with a name and the expressions that define the index. This will immediately
index all documents already in the database, and afterwards any that are added
or updated.

.. code-block:: python

        def get_all_tags(self):
            """Get all tags in use in the entire database."""
            return [key[0] for key in self.db.get_index_keys(TAGS_INDEX)]

The py:meth:`~u1db.Database.get_index_keys` method gets a list of all indexed
*values* from an index. In this case it will give us a list of all tags that
have been used in the database, which can be useful if we want to present them
in the user interface of our application.

.. code-block:: python

        def get_tasks_by_tags(self, tags):
            """Get all tasks that have every tag in tags."""
            if not tags:
                # No tags specified, so return all tasks.
                return self.get_all_tasks()
            # Get all tasks for the first tag.
            results = dict(
                (doc.doc_id, doc) for doc in
                self.db.get_from_index(TAGS_INDEX, tags[0]))
            # Now loop over the rest of the tags (if any) and remove from the
            # results any document that does not have that particular tag.
            for tag in tags[1:]:
                # Get the ids of all documents with this tag.
                ids = [
                    doc.doc_id for doc in self.db.get_from_index(TAGS_INDEX, tag)]
                for key in results.keys():
                    if key not in ids:
                        # Remove the document from result, because it does not have
                        # this particular tag.
                        del results[key]
                        if not results:
                            # If results is empty, we're done: there are no
                            # documents with all tags.
                            return []
            return results.values()

This method gives us a way to query the database by a set of tags. We loop
through the tags one by one and then filter out any documents that don't have
that particular tag.

.. code-block:: python

        def get_task(self, doc_id):
            """Get a task from the database."""
            task = self.db.get_doc(doc_id)
            if task is None:
                # No document with that id exists in the database.
                raise KeyError("No task with id '%s'." % (doc_id,))
            if task.is_tombstone():
                # The document id exists, but the document's content was previously
                # deleted.
                raise KeyError("Task with id %s was deleted." % (doc_id,))
            return task

``get_task`` is a thin wrapper around :py:meth:`~u1db.Database.get_doc` that
takes care of raising appropriate exceptions when a document does not exist or
has been deleted. (Deleted documents leave a 'tombstone' behind, which is
necessary to make sure that synchronisation of the database with other replicas
does the right thing.)

