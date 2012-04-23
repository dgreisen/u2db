"""User interface for the u1todo example application."""

from u1db.backends import inmemory
from u1todo import TodoStore
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

    def __init__(self):
        super(Main, self).__init__()
        uifile = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              'u1todo.ui')
        uic.loadUi(uifile, self)
        db = inmemory.InMemoryDatabase("u1todo")
        self.store = TodoStore(db)
        self.connect_events()
        self.item = None
        self.delete_button.setEnabled(False)

    def connect_events(self):
        """Hook up all the signal handlers."""
        self.connect(
            self.edit_button, QtCore.SIGNAL("clicked()"), self.update)
        self.connect(
            self.delete_button, QtCore.SIGNAL("clicked()"), self.delete)
        self.connect(
            self.list_widget, QtCore.SIGNAL("currentRowChanged(int)"),
            self.row_changed)

    def update(self):
        """Either add a new task or update an existing one."""
        text = unicode(self.line_edit.text(), 'utf-8')
        if not text:
            return
        if self.item is None:
            self.add_item(text)
        else:
            self.update_item(text)
        self.line_edit.clear()
        self.item = None

    def delete(self):
        """Delete a todo item."""
        item = self.list_widget.takeItem(self.list_widget.currentRow())
        self.store.delete_task(item.task)
        if self.list_widget.count() == 0:
            self.delete_button.setEnabled(False)

    def add_item(self, text):
        """Add a new todo item."""
        task = self.store.new_task(text)
        item = UITask(task)
        self.list_widget.addItem(item)
        self.delete_button.setEnabled(True)

    def update_item(self, text):
        """Edit an existing todo item."""
        self.item.task.title = text
        self.item.setText(text)
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
