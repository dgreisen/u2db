import json

DONE = 'x'
NOT_DONE = ''

EMPTY_TASK = json.dumps({"title": "", "done": NOT_DONE, "tags": []})

INDEXES = {
    'tags': ['tags'],
    'done': ['done'],
}


class TodoStore(object):
    """The todo application backend."""

    def __init__(self, db):
        db_indexes = dict(db.list_indexes())
        for name, expression in INDEXES.items():
            if name not in db_indexes:
                # The index does not yet exist.
                db.create_index(name, expression)
                continue
            if expression == db_indexes[name]:
                # The index exists and is up to date.
                continue
            # The index exists but the definition is out of date.
            db.delete_index(name)
            db.create_index(name, expression)

    def tag_task(self, task, tags):
        """Set the tags of a task."""
        task.tags = tags


class Task(object):
    """A todo item."""

    def __init__(self, db, doc_id=None):
        self.db = db
        if doc_id:
            # We are looking for an existing document.
            self.doc_id = doc_id
            self._document = self.db.get_doc(doc_id)
            if self._document is None:
                raise KeyError("No task with id '%s'." % (doc_id,))
        else:
            # Create a new empty Task document in the database.
            self._document = self.db.create_doc(content=EMPTY_TASK)
            self.doc_id = self._document.doc_id
        # Convert the document content that we got from the database (or that
        # we just created,) to a Python object, and cache it as a private
        # property.
        self._content = json.loads(self._document.content)

    def _save(self):
        """Save the document to the database."""
        # Convert the cached content to a json string, and save it to the
        # database. This should be called after any modification of a task that
        # the UI expects to be persistent.
        self._document.content = json.dumps(self._content)
        self.db.put_doc(self._document)

    def _get_title(self):
        """Get the task title."""
        return self._content['title']

    def _set_title(self, title):
        """Set the task title and save to db."""
        self._content['title'] = title
        self._save()

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
        self._save()

    done = property(_get_done, _set_done, doc="Done flag.")

    def _get_tags(self):
        """Get tags associated with the task."""
        return self._content['tags']

    def _set_tags(self, tags):
        self._content['tags'] = list(set(tags))
        self._save()

    tags = property(_get_tags, _set_tags, doc="Task tags.")

    def add_tag(self, tag):
        tags = self._content['tags']
        if tag in tags:
            # Tasks cannot have the same tag more than once, so ignore the
            # request to add it again.
            return
        tags.append(tag)
        self._save()

    def remove_tag(self, tag):
        tags = self._content['tags']
        if tag not in tags:
            # Can't remove a tag that the task does not have.
            raise KeyError("Task has no tag '%s'." % (tag,))
        tags.remove(tag)
        self._save()
