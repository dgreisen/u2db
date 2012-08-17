Tutorial
########

In this tutorial we will demonstrate what goes into creating an application
that uses u1db as a backend. We will use code from the simple todo list
application 'Cosas' as our example. The full source code to Cosas can be found
in the u1db source tree.  It comes with a user interface, but we will only
focus on the code that interacts with u1db here.

Tasks
-----

First we'll define what we'll actually store in u1db. For a todo list
application, it makes sense to have each todo item (or task,) be a single
document in the database, so that we can use indexes to find individual tasks
with specific properties.


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

.. testcode ::

    example_task = Task()
    example_task.title = "Create a Task class."
    print(example_task.title)

.. testoutput ::

    Create a Task class.

.. testcode ::

    print(example_task.tags)

.. testoutput ::

    []

.. testcode ::

    example_task.tags = ['develoment']
    print(example_task.tags)

.. testoutput ::

    ['develoment']

.. testcode ::

    print(example_task.done)

.. testoutput ::

    False

.. testcode ::

    example_task.done = True
    print(example_task.done)

.. testoutput ::

    True
