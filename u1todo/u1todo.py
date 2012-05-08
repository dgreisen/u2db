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

"""u1todo example application."""

import json
import os
import re
import xdg.BaseDirectory
import u1db

EMPTY_TASK = json.dumps({"title": "", "done": False, "tags": []})

TAGS_INDEX = 'tags'
DONE_INDEX = 'done'
INDEXES = {
    TAGS_INDEX: ['tags'],
    DONE_INDEX: ['bool(done)'],
}

TAGS = re.compile('#(\w+)|\[(.+)\]')


def get_database():
    """Get the path that the database is stored in."""
    return u1db.open(
        os.path.join(xdg.BaseDirectory.save_data_path("u1todo"),
        "u1todo.u1db"), create=True)


def extract_tags(text):
    """Extract the tags from the text."""
    return [t[0] if t[0] else t[1] for t in TAGS.findall(text)]


class TodoStore(object):
    """The todo application backend."""

    def __init__(self, db):
        self.db = db

    def initialize_db(self):
        """Initialize the database."""
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

    def get_all_tags(self):
        """Get all the tags in use."""
        return self.db.get_index_keys(TAGS_INDEX)

    def get_tasks_by_tags(self, tags):
        if not tags:
            return self.get_all_tasks()
        results = {
            doc.doc_id: doc for doc in
            self.db.get_from_index(TAGS_INDEX, [(tags[0],)])}
        for tag in tags[1:]:
            ids = [
                doc.doc_id for doc in
                self.db.get_from_index(TAGS_INDEX, [(tag,)])]
            for key in results.keys():
                if key not in ids:
                    del results[key]
                    if not results:
                        return []
        return [Task(doc) for doc in results.values()]

    def get_task(self, task_id):
        """Get a task from the database."""
        document = self.db.get_doc(task_id)
        if document is None:
            raise KeyError("No task with id '%s'." % (task_id,))
        if document.content is None:
            raise KeyError("Task with id %s was deleted." % (task_id,))
        return Task(document)

    def delete_task(self, task):
        """Delete a task from the database."""
        self.db.delete_doc(task._document)

    def new_task(self, title=None, tags=None):
        """Create a new task document."""
        # Create the document in the u1db database
        if tags is None:
            tags = []
        content = EMPTY_TASK
        if title or tags:
            content_object = json.loads(content)
            content_object['title'] = title
            content_object['tags'] = tags
            content = json.dumps(content_object)
        document = self.db.create_doc(content=content)
        # Wrap the document in a Task object.
        return Task(document)

    def save_task(self, task):
        """Save task to the database."""
        # Get the u1db document from the task object, and save it to the
        # database.
        self.db.put_doc(task.document)

    def get_all_tasks(self):
        return [
            Task(doc) for doc in self.db.get_from_index(DONE_INDEX, ["*"])]


class Task(object):
    """A todo item."""

    def __init__(self, document):
        self._document = document
        self._content = json.loads(document.content)

    @property
    def task_id(self):
        """The u1db id of the task."""
        return self._document.doc_id

    def _get_title(self):
        """Get the task title."""
        return self._content['title']

    def _set_title(self, title):
        """Set the task title and save to db."""
        self._content['title'] = title

    title = property(_get_title, _set_title, doc="Title of the task.")

    def _get_done(self):
        """Get the status of the task."""
        return self._content['done']

    def _set_done(self, value):
        """Set the done status."""
        self._content['done'] = value

    done = property(_get_done, _set_done, doc="Done flag.")

    def _get_tags(self):
        """Get tags associated with the task."""
        return self._content['tags']

    def _set_tags(self, tags):
        """Set tags associated with the task."""
        self._content['tags'] = list(set(tags))

    tags = property(_get_tags, _set_tags, doc="Task tags.")

    @property
    def document(self):
        """The u1db document representing this task."""
        self._document.content = json.dumps(self._content)
        return self._document
