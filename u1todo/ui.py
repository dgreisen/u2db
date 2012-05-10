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

"""User interface for the u1todo example application."""

from collections import defaultdict
from datetime import datetime
import os
import sys
from PyQt4 import QtGui, QtCore, uic

from u1todo import TodoStore, get_database, extract_tags
import u1db
from u1db.errors import DatabaseDoesNotExist
from u1db.sync import Synchronizer
from u1db.remote.http_target import HTTPSyncTarget
from u1db.remote.http_database import HTTPDatabase
from ubuntuone.platform.credentials import CredentialsManagementTool


class UITask(QtGui.QTreeWidgetItem):
    """Task list item."""

    def __init__(self, task):
        super(UITask, self).__init__()
        self.task = task
        # Set the list item's text to the task's title.
        self.setText(0, self.task.title)
        # If the task is done, check off the list item.
        self.setCheckState(
            0, QtCore.Qt.Checked if task.done else QtCore.Qt.Unchecked)
        self.update_strikethrough()

    def update_strikethrough(self):
        font = self.font(0)
        font.setStrikeOut(self.task.done)
        self.setFont(0, font)


class Main(QtGui.QMainWindow):
    """Main window of our application."""

    def __init__(self, in_memory=False):
        super(Main, self).__init__()
        # Dynamically load the ui file generated by QtDesigner.
        uifile = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), 'u1todo.ui')
        uic.loadUi(uifile, self)
        # hook up the signals to the signal handlers.
        self.connect_events()
        # Load the u1todo database.
        db = get_database()
        # And wrap it in a TodoStore object.
        self.store = TodoStore(db)
        # create or update the indexes if they are not up-to-date
        self.store.initialize_db()
        # Initially the delete button is disabled, because there are no tasks
        # to delete.
        self.delete_button.setEnabled(False)
        # Initialize some variables we will use to keep track of the tags.
        self._tag_docs = defaultdict(list)
        self._tag_buttons = {}
        self._tag_filter = []
        # Get all the tasks in the database, and add them to the UI.
        for task in self.store.get_all_tasks():
            self.add_task(task)
        self.task_edit.clear()
        # Give the edit field focus.
        self.task_edit.setFocus()
        # Initialize the variable that points to the currently selected list
        # item.
        self.item = None

    def connect_events(self):
        """Hook up all the signal handlers."""
        # On enter, save the task that was being edited.
        self.task_edit.returnPressed.connect(self.update)
        # When the Edit/Add button is clicked, save the task that was being
        # edited.
        self.edit_button.clicked.connect(self.update)
        # When the Delete button is clicked, delete the currently selected task
        # (if any.)
        self.delete_button.clicked.connect(self.delete)
        # If a new row in the list is selected, change the currently selected
        # task, and put its contents in the edit field.
        self.todo_list.currentItemChanged.connect(self.row_changed)
        # If the checked status of an item in the list changes, change the done
        # status of the task.
        self.todo_list.itemChanged.connect(self.item_changed)
        self.sync_button.clicked.connect(self.synchronize)

    def refresh_filter(self):
        """Remove all tasks, and show only those that satisfy the new filter.

        """
        # Remove everything from the list.
        while len(self.todo_list):
            self.todo_list.takeItem(0)
        # Get the filtered tasks from the database.
        for task in self.store.get_tasks_by_tags(self._tag_filter):
            # Add them to the UI.
            self.add_task(task)
        # Clear the current selection.
        self.todo_list.setCurrentRow(-1)
        self.task_edit.clear()
        self.item = None

    def item_changed(self, item):
        """Mark a task as done or not done."""
        if item.checkState() == QtCore.Qt.Checked:
            item.task.done = True
        else:
            item.task.done = False
        # Save the task to the database.
        item.update_strikethrough()
        item.setText(item.task.title)
        self.store.save_task(item.task)
        # Clear the current selection.
        self.todo_list.setCurrentRow(-1)
        self.task_edit.clear()
        self.item = None

    def update(self):
        """Either add a new task or update an existing one."""
        text = unicode(self.task_edit.text(), 'utf-8')
        if not text:
            # There was no text in the edit field so do nothing.
            return
        if self.item is None:
            # No task was selected, so add a new one.
            task = self.store.new_task(text, tags=extract_tags(text))
            self.add_task(task)
        else:
            # A task was selected, so update it.
            self.update_task_text(text)
        # Clear the current selection.
        self.todo_list.setCurrentRow(-1)
        self.task_edit.clear()
        self.item = None

    def get_ubuntuone_credentials(self):
        cmt = CredentialsManagementTool()
        return cmt.find_credentials()

    def synchronize(self):
        self.sync_button.setEnabled(False)
        if self.u1_radio.isChecked():
            d = self.get_ubuntuone_credentials()
            d.addCallback(self._synchronize)
        else:
            # TODO: add ui for entering creds for non u1 servers.
            self._synchronize()

    def _synchronize(self, creds=None):
        if self.u1_radio.isChecked():
            # TODO: not hardcode
            target = 'https://u1db.one.ubuntu.com/~/u1todo'
        else:
            target = self.url_edit.text()
        if target.startswith('http://') or target.startswith('https://'):
            st = HTTPSyncTarget.connect(target)
            oauth_creds = {
                'token_key': creds['token'],
                'token_secret': creds['token_secret'],
                'consumer_key': creds['consumer_key'],
                'consumer_secret': creds['consumer_secret']}
            if creds:
                st.set_oauth_credentials(**oauth_creds)
        else:
            db = u1db.open(target, create=True)
            st = db.get_sync_target()
        syncer = Synchronizer(self.store.db, st)
        try:
            syncer.sync()
        except DatabaseDoesNotExist:
            # The server does not yet have the database, so create it.
            if target.startswith('http://') or target.startswith('https://'):
                db = HTTPDatabase(target)
                db.set_oauth_credentials(**oauth_creds)
                db.open(create=True)
            syncer.sync()
        self.refresh_filter()
        self.last_synced.setText(
            '<span style="color:green">%s</span>' % (datetime.now()))
        self.sync_button.setEnabled(True)

    def delete(self):
        """Delete a todo item."""
        # Delete the item from the database.
        row = self.todo_list.currentRow()
        item = self.todo_list.takeItem(row)
        if item is None:
            return
        self.store.delete_task(item.task)
        # Clear the current selection.
        self.todo_list.setCurrentRow(-1)
        self.task_edit.clear()
        self.item = None
        if self.todo_list.count() == 0:
            # If there are no tasks left, disable the delete button.
            self.delete_button.setEnabled(False)

    def add_task(self, task):
        """Add a new todo item."""
        # Wrap the task in a UITask object.
        item = UITask(task)
        self.todo_list.addTopLevelItem(item)
        # We know there is at least one item now so we enable the delete
        # button.
        self.delete_button.setEnabled(True)
        if not task.tags:
            return
        # If the task has tags, we add them as filter buttons to the UI, if
        # they are new.
        for tag in task.tags:
            self.add_tag(task.task_id, tag)

    def add_tag(self, task_id, tag):
        """Create a link between the task with id task_id and the tag, and
        add a new button for tag if it was not already there.

        """
        # Add the task id to the list of document ids associated with this tag.
        self._tag_docs[tag].append(task_id)
        # If the list has more than one element the tag button was already
        # present.
        if len(self._tag_docs[tag]) > 1:
            return
        # Add a tag filter button for this tag to the UI.
        button = QtGui.QPushButton(tag)
        button._u1todo_tag = tag
        # Make the button an on/off button.
        button.setCheckable(True)
        # Store a reference to the button in a dictionary so we can find it
        # back more easily if we need to delete it.
        self._tag_buttons[tag] = button

        # We define a function to handle the clicked signal of the button,
        # since each button will need its own handler.
        def filter_toggle(checked):
            """Toggle the filter for the tag associated with this button."""
            if checked:
                # Add the tag to the current filter.
                self._tag_filter.append(button._u1todo_tag)
            else:
                # Remove the tag from the current filter.
                self._tag_filter.remove(button._u1todo_tag)
            # Apply the new filter.
            self.refresh_filter()

        # Attach the handler to the button's clicked signal.
        button.clicked.connect(filter_toggle)
        # Get the position where the button needs to be inserted. (We keep them
        # sorted alphabetically by the text of the tag.
        index = sorted(self._tag_buttons.keys()).index(tag)
        # And add the button to the UI.
        self.tag_buttons.insertWidget(index, button)

    def remove_tag(self, task_id, tag):
        """Remove the link between the task with id task_id and the tag, and
        remove the button for tag if it no longer has any tasks associated with
        it.

        """
        # Remove the task id from the list of document ids associated with this
        # tag.
        self._tag_docs[tag].remove(task_id)
        # If the list is not empty, we do not remove the button, because there
        # are still tasks that have this tag.
        if self._tag_docs[tag]:
            return
        # Look up the button.
        button = self._tag_buttons[tag]
        # Remove it from the ui.
        self.tag_buttons.removeWidget(button)
        # And remove the reference.
        del self._tag_buttons[tag]

    def update_tags(self, task_id, old_tags, new_tags):
        """Process any changed tags for this task_id."""
        # Process all removed tags.
        for tag in old_tags - new_tags:
            self.remove_tag(task_id, tag)
        # Process all tags newly added.
        for tag in new_tags - old_tags:
            self.add_tag(task_id, tag)

    def update_task_text(self, text):
        """Edit an existing todo item."""
        item = self.item
        task = item.task
        # Change the task's title to the text in the edit field.
        task.title = text
        # Record the current tags.
        old_tags = set(task.tags) if task.tags else set([])
        # Extract the new tags from the new text.
        new_tags = set(extract_tags(text))
        # Check if the tag filter buttons need updating.
        self.update_tags(item.task.task_id, old_tags, new_tags)
        # Set the tags on the task.
        task.tags = list(new_tags)
        # disconnect the signal temporarily while we change the title
        self.todo_list.itemChanged.disconnect(self.item_changed)
        # Change the text in the UI.
        item.setText(text)
        # reconnect the signal after we changed the title
        self.todo_list.itemChanged.connect(self.item_changed)
        # Save the changed task to the database.
        self.store.save_task(task)

    def row_changed(self, index):
        """Edit item when row changes."""
        if index == -1:
            self.task_edit.clear()
            return
        # If a row is selected, show the selected task's title in the edit
        # field.
        self.item = self.todo_list.item(index)
        self.task_edit.setText(self.item.task.title)


if __name__ == "__main__":
    # Unfortunately, to be able to use ubuntuone.platform.credentials on linux,
    # we now depend on dbus. :(
    from dbus.mainloop.qt import DBusQtMainLoop
    main_loop = DBusQtMainLoop(set_as_default=True)
    app = QtGui.QApplication(sys.argv)
    main = Main()
    main.show()
    app.exec_()
