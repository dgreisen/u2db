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

from u1todo import TodoStore, get_database, extract_tags
from collections import defaultdict
import os
import sys
from PyQt4 import QtGui, QtCore, uic


class UITask(QtGui.QListWidgetItem):
    """Task list item."""

    def __init__(self, task):
        super(UITask, self).__init__()
        self.task = task
        self.setText(self.task.title)
        self.setCheckState(
            QtCore.Qt.Checked if task.done else QtCore.Qt.Unchecked)


class Main(QtGui.QMainWindow):
    """Main window of our application."""

    def __init__(self, in_memory=False):
        super(Main, self).__init__()
        uifile = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              'u1todo.ui')
        uic.loadUi(uifile, self)
        self.connect_events()
        db = get_database()
        self.store = TodoStore(db)
        # create or update the indexes if they are not up-to-date
        self.store.initialize_db()
        self.delete_button.setEnabled(False)
        self._tag_docs = defaultdict(list)
        self._tag_buttons = {}
        self._tag_filter = []
        for task in self.store.get_all_tasks():
            self.add_task(task)
        self.task_edit.clear()
        self.task_edit.setFocus()
        self.item = None

    def connect_events(self):
        """Hook up all the signal handlers."""
        self.task_edit.returnPressed.connect(self.update)
        self.edit_button.clicked.connect(self.update)
        self.delete_button.clicked.connect(self.delete)
        self.todo_list.currentRowChanged.connect(self.row_changed)
        self.todo_list.itemChanged.connect(self.item_changed)

    def refresh_filter(self):
        while len(self.todo_list):
            self.todo_list.takeItem(0)
        for task in self.store.get_tasks_by_tags(self._tag_filter):
            self.add_task(task)
        self.item = None
        self.task_edit.clear()

    def item_changed(self, item):
        if item.checkState() == QtCore.Qt.Checked:
            item.task.done = True
        else:
            item.task.done = False
        self.store.save_task(item.task)
        self.todo_list.setCurrentRow(-1)
        self.task_edit.clear()
        self.item = None

    def update(self):
        """Either add a new task or update an existing one."""
        text = unicode(self.task_edit.text(), 'utf-8')
        if not text:
            return
        if self.item is None:
            task = self.store.new_task(text, tags=extract_tags(text))
            self.add_task(task)
        else:
            self.update_task_text(text)
        self.todo_list.setCurrentRow(-1)
        self.task_edit.clear()
        self.item = None

    def delete(self):
        """Delete a todo item."""
        item = self.todo_list.takeItem(self.todo_list.currentRow())
        self.store.delete_task(item.task)
        self.todo_list.setCurrentRow(-1)
        if self.todo_list.count() == 0:
            self.delete_button.setEnabled(False)

    def add_task(self, task):
        """Add a new todo item."""
        item = UITask(task)
        self.todo_list.addItem(item)
        self.delete_button.setEnabled(True)
        if not task.tags:
            return
        for tag in task.tags:
            self.add_tag(task.task_id, tag)

    def add_tag(self, task_id, tag):
        self._tag_docs[tag].append(task_id)
        if len(self._tag_docs[tag]) > 1:
            return
        button = QtGui.QPushButton(tag)
        button._u1todo_tag = tag
        button.setCheckable(True)
        self._tag_buttons[tag] = button

        def filter_toggle(checked):
            if checked:
                self._tag_filter.append(button._u1todo_tag)
            else:
                self._tag_filter.remove(button._u1todo_tag)
            self.refresh_filter()

        button.clicked.connect(filter_toggle)
        index = sorted(self._tag_buttons.keys()).index(tag)
        self.tag_buttons.insertWidget(index, button)

    def remove_tag(self, task_id, tag):
        self._tag_docs[tag].remove(task_id)
        if self._tag_docs[tag]:
            return
        button = self._tag_buttons[tag]
        self.tag_buttons.removeWidget(button)
        del self._tag_buttons[tag]

    def update_tags(self, task_id, old_tags, new_tags):
        for tag in old_tags - new_tags:
            self.remove_tag(task_id, tag)
        for tag in new_tags - old_tags:
            self.add_tag(task_id, tag)

    def update_task_text(self, text):
        """Edit an existing todo item."""
        item = self.item
        task = item.task
        task.title = text
        old_tags = set(task.tags) if task.tags else set([])
        new_tags = set(extract_tags(text))
        self.update_tags(item.task.task_id, old_tags, new_tags)
        task.tags = list(new_tags)
        # disconnect the signal temporarily while we change the title
        self.todo_list.itemChanged.disconnect(self.item_changed)
        item.setText(text)
        # reconnect the signal after we changed the title
        self.todo_list.itemChanged.connect(self.item_changed)
        self.store.save_task(task)

    def row_changed(self, index):
        """Edit item when row changes."""
        if index == -1:
            self.task_edit.clear()
            return
        self.edit_item(self.todo_list.item(index))

    def edit_item(self, item):
        """Edit item in task edit box."""
        self.item = item
        self.task_edit.setText(item.task.title)


if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    main = Main()
    main.show()
    app.exec_()
