# Copyright 2012 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

"""Tests for u1todo example application."""

from testtools import TestCase
from u1todo import Task, TodoStore, INDEXES, EMPTY_TASK, DONE, NOT_DONE
from u1db.backends import inmemory


class TodoStoreTestCase(TestCase):

    def setUp(self):
        super(TodoStoreTestCase, self).setUp()
        self.db = inmemory.InMemoryDatabase("u1todo")

    def test_initialize_db(self):
        """Creates indexes."""
        store = TodoStore(self.db)
        store.initialize_db()
        self.assertEqual(INDEXES, dict(self.db.list_indexes()))

    def test_reinitialize_db(self):
        """Creates indexes."""
        store = TodoStore(self.db)
        store.new_task()
        store.initialize_db()
        self.assertEqual(INDEXES, dict(self.db.list_indexes()))

    def test_indexes_are_added(self):
        """New indexes are added when a new store is created."""
        store = TodoStore(self.db)
        store.initialize_db()
        INDEXES['foo'] = ['bar']
        self.assertNotIn('foo', dict(self.db.list_indexes()))
        store = TodoStore(self.db)
        store.initialize_db()
        self.assertIn('foo', dict(self.db.list_indexes()))

    def test_indexes_are_updated(self):
        """Indexes are updated when a new store is created."""
        store = TodoStore(self.db)
        store.initialize_db()
        new_expression = ['newtags']
        INDEXES['tags'] = new_expression
        self.assertNotEqual(
            new_expression, dict(self.db.list_indexes())['tags'])
        store = TodoStore(self.db)
        store.initialize_db()
        self.assertEqual(new_expression, dict(self.db.list_indexes())['tags'])

    def test_tag_task(self):
        """Sets the tags for a task."""
        store = TodoStore(self.db)
        task = store.new_task()
        tag = "you're it"
        store.tag_task(task, [tag])
        self.assertEqual([tag], task.tags)

    def test_new_task(self):
        """Creates a new task."""
        store = TodoStore(self.db)
        task = store.new_task()
        self.assertTrue(isinstance(task, Task))
        self.assertIsNotNone(task.task_id)

    def test_new_task_with_title(self):
        """Creates a new task."""
        store = TodoStore(self.db)
        title = "Un task muy importante"
        task = store.new_task(title=title)
        self.assertEqual(title, task.title)

    def test_new_task_with_tags(self):
        """Creates a new task."""
        store = TodoStore(self.db)
        tags = ['foo', 'bar', 'bam']
        task = store.new_task(tags=tags)
        self.assertEqual(tags, task.tags)

    def test_save_task_get_task(self):
        """Saves a modified task and retrieves it from the db."""
        store = TodoStore(self.db)
        task = store.new_task()
        task.title = "This is the title."
        store.save_task(task)
        task_copy = store.get_task(task.task_id)
        self.assertEqual(task.title, task_copy.title)

    def test_get_non_existant_task(self):
        """Saves a modified task and retrieves it from the db."""
        store = TodoStore(self.db)
        self.assertRaises(KeyError, store.get_task, "nonexistant")

    def test_delete_task(self):
        """Deletes a task by id."""
        store = TodoStore(self.db)
        task = store.new_task()
        store.delete_task(task)
        self.assertRaises(KeyError, store.get_task, task.task_id)

    def test_get_all_tasks(self):
        store = TodoStore(self.db)
        store.initialize_db()
        task1 = store.new_task()
        task2 = store.new_task()
        task3 = store.new_task()
        task_ids = [task.task_id for task in store.get_all_tasks()]
        self.assertEqual(
            sorted([task1.task_id, task2.task_id, task3.task_id]),
            sorted(task_ids))


class TaskTestCase(TestCase):
    """Tests for Task."""

    def setUp(self):
        super(TaskTestCase, self).setUp()
        self.db = inmemory.InMemoryDatabase("u1todo")
        self.document = self.db.create_doc(EMPTY_TASK)

    def test_task(self):
        """Initializing a task."""
        task = Task(self.document)
        self.assertEqual("", task.title)
        self.assertEqual([], task.tags)
        self.assertEqual(False, task.done)

    def test_task_id(self):
        """Task id is set to document id."""
        task = Task(self.document)
        self.assertEqual(self.document.doc_id, task.task_id)

    def test_set_title(self):
        """Changing the title is persistent."""
        task = Task(self.document)
        title = "new task"
        task.title = title
        self.assertEqual(title, task._content['title'])

    def test_set_done(self):
        """Changing the done property changes the underlying content."""
        task = Task(self.document)
        self.assertEqual(NOT_DONE, task._content['done'])
        task.done = True
        self.assertEqual(DONE, task._content['done'])

    def test_tags(self):
        """Tags property returns a list."""
        task = Task(self.document)
        self.assertEqual([], task.tags)

    def set_tags(self):
        """Setting the tags property changes the underlying content."""
        task = Task(self.document)
        task.tags = ["foo", "bar"]
        self.assertEqual(["foo", "bar"], task._content['tags'])

    def test_add_tag(self):
        """Tag is added to task's tags."""
        task = Task(self.document)
        task.add_tag("foo")
        self.assertEqual(["foo"], task.tags)

    def test_remove_tag(self):
        """Tag is removed from task's tags."""
        task = Task(self.document)
        task.add_tag("foo")
        task.add_tag("bar")
        self.assertEqual(["foo", "bar"], task.tags)
        task.remove_tag("foo")
        self.assertEqual(["bar"], task.tags)