Tutorial
########

In this tutorial we will demonstrate what goes into creating an application
that uses u1db as a backend. We will use code from the simple todo list
application 'Cosas' as our example. The full source code to Cosas can be found
in the u1db source tree.  It comes with a user interface, but we will only
focus on the code that interacts with u1db here.

Tasks
-----

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

    '''
    {
     "title": "the task at hand",
     "done": false,
     "tags": ["urgent", "priority 1", "today"]
    }
    '''

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

.. doctest ::

    >>> example_task = Task()
    >>> example_task.title = "Create a Task class."
    >>> example_task.title
    'Create a Task class.'

their tags:

.. doctest ::

    >>> example_task.tags
    []

.. doctest ::

    >>> example_task.tags = ['develoment']
    >>> example_task.tags
    ['develoment']

and their done status:

.. doctest ::

    >>> example_task.done
    False

.. doctest ::

    >>> example_task.done = True
    >>> example_task.done
    True

