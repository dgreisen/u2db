import json

DONE = 'true'
NOT_DONE = 'false'

EMPTY_TASK = json.dumps({"title": "", "done": NOT_DONE, "tags": []})

INDEXES = {
    'tags': ['tags'],
    'done': ['done'],
}


class TodoStore(object):
    """The todo application backend."""

    def __init__(self, db):
        self.db = db
        self.initialize_db()

    def initialize_db(self):
        db_indexes = dict(self.db.list_indexes())
        for name, expression in INDEXES.items():
            if name not in db_indexes:
                # The index does not yet exist.
                self.db.create_index(name, expression)
                continue
            if expression == db_indexes[name]:
                # The index exists and is up to date.
                continue
            # The index exists but the definition is out of date.
            self.db.delete_index(name)
            self.db.create_index(name, expression)

    def tag_task(self, task, tags):
        """Set the tags of a task."""
        task.tags = tags

    def get_task(self, task_id):
        """Get a task from the database."""
        document = self.db.get_doc(task_id)
        if document is None:
            raise KeyError("No task with id '%s'." % (task_id,))
        return Task(document)

    def new_task(self):
        document = self.db.create_doc(content=EMPTY_TASK)
        return Task(document)

    def save_task(self, task):
        """Save task to the database."""
        self.db.put_doc(task.document)


class Task(object):
    """A todo item."""

    def __init__(self, document):
        self._document = document
        self._content = json.loads(document.content)

    def _get_title(self):
        """Get the task title."""
        return self._content['title']

    def _set_title(self, title):
        """Set the task title and save to db."""
        self._content['title'] = title

    title = property(_get_title, _set_title, doc="Title of the task.")

    def _get_done(self):
        """Get the status of the task."""
        # Indexes on booleans are not currently possible, so we convert to and
        # from strings.
        return True if self._content['done'] == DONE else False

    def _set_done(self, value):
        # Indexes on booleans are not currently possible, so we convert to and
        # from strings.
        self._content['done'] = DONE if value else NOT_DONE

    done = property(_get_done, _set_done, doc="Done flag.")

    def _get_tags(self):
        """Get tags associated with the task."""
        return self._content['tags']

    def _set_tags(self, tags):
        self._content['tags'] = list(set(tags))

    tags = property(_get_tags, _set_tags, doc="Task tags.")

    def add_tag(self, tag):
        tags = self._content['tags']
        if tag in tags:
            # Tasks cannot have the same tag more than once, so ignore the
            # request to add it again.
            return
        tags.append(tag)

    def remove_tag(self, tag):
        tags = self._content['tags']
        if tag not in tags:
            # Can't remove a tag that the task does not have.
            raise KeyError("Task has no tag '%s'." % (tag,))
        tags.remove(tag)

    @property
    def document(self):
        self._document.content = json.dumps(self._content)
        return self._document
