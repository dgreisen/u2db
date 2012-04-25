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

from u1todo import TodoStore, get_database
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
        db = get_database()
        self.store = TodoStore(db)
        self.store.initialize_db()
        self.connect_events()
        self.item = None
        self.delete_button.setEnabled(False)
        for task in self.store.get_all_tasks():
            self.add_task(task)

    def connect_events(self):
        """Hook up all the signal handlers."""
        self.edit_button.clicked.connect(self.update)
        self.delete_button.clicked.connect(self.delete)
        self.list_widget.currentRowChanged.connect(self.row_changed)
        self.list_widget.itemChanged.connect(self.item_changed)

    def item_changed(self, item):
        if item.checkState() == QtCore.Qt.Checked:
            item.task.done = True
        else:
            item.task.done = False
        self.store.save_task(item.task)

    def update(self):
        """Either add a new task or update an existing one."""
        text = unicode(self.line_edit.text(), 'utf-8')
        if not text:
            return
        if self.item is None:
            task = self.store.new_task(text)
            self.add_task(task)
        else:
            self.update_task_text(text)
        self.line_edit.clear()
        self.item = None

    def delete(self):
        """Delete a todo item."""
        item = self.list_widget.takeItem(self.list_widget.currentRow())
        self.store.delete_task(item.task)
        if self.list_widget.count() == 0:
            self.delete_button.setEnabled(False)

    def add_task(self, task):
        """Add a new todo item."""
        item = UITask(task)
        self.list_widget.addItem(item)
        self.delete_button.setEnabled(True)

    def update_task_text(self, text):
        """Edit an existing todo item."""
        self.item.task.title = text
        # disconnect the signal temporarily while we change the title
        self.list_widget.itemChanged.disconnect(self.item_changed)
        self.item.setText(text)
        # reconnect the signal after we changed the title
        self.list_widget.itemChanged.connect(self.item_changed)
        self.store.save_task(self.item.task)

    def row_changed(self, index):
        """Edit item when row changes."""
        if index == -1:
            self.line_edit.clear()
            return
        self.edit_item(self.list_widget.item(index))

    def edit_item(self, item):
        """Edit item in line edit box."""
        self.item = item
        self.line_edit.setText(item.task.title)


if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    main = Main()
    main.show()
    app.exec_()
