from testtools import TestCase
from u1todo import Task, TodoStore, INDEXES, DONE
from u1db.backends import inmemory


class TodoStoreTestCase(TestCase):

    def setUp(self):
        super(TodoStoreTestCase, self).setUp()
        self.db = inmemory.InMemoryDatabase("u1todo")

    def test_initialize_db(self):
        """Creates indexes."""
        TodoStore(self.db)
        self.assertEqual(INDEXES, dict(self.db.list_indexes()))

    def test_indexes_are_added(self):
        """New indexes are added when a new store is created."""
        TodoStore(self.db)
        INDEXES['foo'] = ['bar']
        self.assertNotIn('foo', dict(self.db.list_indexes()))
        TodoStore(self.db)
        self.assertIn('foo', dict(self.db.list_indexes()))

    def test_indexes_are_updated(self):
        """Indexes are updated when a new store is created."""
        TodoStore(self.db)
        new_expression = ['newtags']
        INDEXES['tags'] = new_expression
        self.assertNotEqual(
            new_expression, dict(self.db.list_indexes())['tags'])
        TodoStore(self.db)
        self.assertEqual(new_expression, dict(self.db.list_indexes())['tags'])

    def test_tag_task(self):
        store = TodoStore(self.db)
        task = Task(self.db)
        tag = "you're it"
        store.tag_task(task, [tag])
        self.assertEqual([tag], task.tags)


class TaskTestCase(TestCase):
    """Tests for Task."""

    def setUp(self):
        super(TaskTestCase, self).setUp()
        self.db = inmemory.InMemoryDatabase("u1todo")

    def test_task(self):
        """Initializing a task generates a doc_id."""
        task = Task(self.db)
        self.assertIsNotNone(task.doc_id)

    def test_set_title(self):
        """Changing the title is persistent."""
        task = Task(self.db)
        title = "new task"
        task.title = title
        new_task = Task(self.db, doc_id=task.doc_id)
        self.assertEqual(title, new_task.title)

    def test_set_done(self):
        task = Task(self.db)
        self.assertFalse(task.done)
        task.done = DONE
        self.assertTrue(task.done)

    def test_set_done_persists(self):
        task = Task(self.db)
        self.assertFalse(task.done)
        task.done = DONE
        new_task = Task(self.db, doc_id=task.doc_id)
        self.assertTrue(new_task.done)

    def test_tags(self):
        """Tags property returns a tuple."""
        task = Task(self.db)
        self.assertEqual([], task.tags)

    def set_tags(self):
        task = Task(self.db)
        task.tags = ["foo", "bar"]
        self.assertEqual(["foo", "bar"], task.tags)

    def set_tags_persists(self):
        """Tags are saved to the database."""
        task = Task(self.db)
        task.tags = ("foo", "bar")
        new_task = Task(self.db, doc_id=task.doc_id)
        self.assertEqual(["foo", "bar"], new_task.tags)

    def test_add_tag(self):
        """Tag is added to task's tags."""
        task = Task(self.db)
        task.add_tag("foo")
        self.assertEqual(["foo"], task.tags)

    def test_add_tag_persists(self):
        """Tag is saved to the database."""
        task = Task(self.db)
        task.add_tag("foo")
        new_task = Task(self.db, doc_id=task.doc_id)
        self.assertEqual(["foo"], new_task.tags)

    def test_remove_tag(self):
        """Tag is removed from task's tags."""
        task = Task(self.db)
        task.add_tag("foo")
        task.add_tag("bar")
        self.assertEqual(["foo", "bar"], task.tags)
        task.remove_tag("foo")
        self.assertEqual(["bar"], task.tags)
