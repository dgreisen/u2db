# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'u1todo/u1todo.ui'
#
# Created: Thu Apr 19 12:42:16 2012
#      by: PyQt4 UI code generator 4.9.1
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui
from u1db.backends import inmemory
from u1todo import TodoStore
import sys

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    _fromUtf8 = lambda s: s


class UIMainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName(_fromUtf8("MainWindow"))
        MainWindow.resize(386, 745)
        self.centralwidget = QtGui.QWidget(MainWindow)
        self.centralwidget.setObjectName(_fromUtf8("centralwidget"))
        self.verticalLayout = QtGui.QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName(_fromUtf8("verticalLayout"))
        self.listWidget = QtGui.QListWidget(self.centralwidget)
        self.listWidget.setAcceptDrops(True)
        self.listWidget.setDragEnabled(False)
        self.listWidget.setAlternatingRowColors(True)
        self.listWidget.setObjectName(_fromUtf8("listWidget"))
        self.verticalLayout.addWidget(self.listWidget)
        self.frame = QtGui.QFrame(self.centralwidget)
        self.frame.setFrameShape(QtGui.QFrame.StyledPanel)
        self.frame.setFrameShadow(QtGui.QFrame.Raised)
        self.frame.setObjectName(_fromUtf8("frame"))
        self.horizontalLayout = QtGui.QHBoxLayout(self.frame)
        self.horizontalLayout.setObjectName(_fromUtf8("horizontalLayout"))
        self.lineEdit = QtGui.QLineEdit(self.frame)
        self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
        self.horizontalLayout.addWidget(self.lineEdit)
        self.updateButton = QtGui.QPushButton(self.frame)
        self.updateButton.setObjectName(_fromUtf8("updateButton"))
        self.horizontalLayout.addWidget(self.updateButton)
        self.deleteButton = QtGui.QPushButton(self.frame)
        self.deleteButton.setObjectName(_fromUtf8("deleteButton"))
        self.horizontalLayout.addWidget(self.deleteButton)
        self.verticalLayout.addWidget(self.frame)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtGui.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 386, 21))
        self.menubar.setObjectName(_fromUtf8("menubar"))
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtGui.QStatusBar(MainWindow)
        self.statusbar.setObjectName(_fromUtf8("statusbar"))
        MainWindow.setStatusBar(self.statusbar)
        self.actionFile = QtGui.QAction(MainWindow)
        self.actionFile.setObjectName(_fromUtf8("actionFile"))

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(
            QtGui.QApplication.translate(
                "MainWindow", "MainWindow", None,
                QtGui.QApplication.UnicodeUTF8))
        self.updateButton.setText(QtGui.QApplication.translate(
            "MainWindow", "Add/Update", None, QtGui.QApplication.UnicodeUTF8))
        self.deleteButton.setText(QtGui.QApplication.translate(
            "MainWindow", "Delete", None, QtGui.QApplication.UnicodeUTF8))
        self.actionFile.setText(QtGui.QApplication.translate(
            "MainWindow", "File", None, QtGui.QApplication.UnicodeUTF8))


class UITask(QtGui.QListWidgetItem):

    def __init__(self, task):
        super(UITask, self).__init__()
        self.task = task
        self.setText(self.task.title)
        self.setCheckState(
            QtCore.Qt.Checked if task.done else QtCore.Qt.Unchecked)


class TodoListApp(QtGui.QApplication):

    def __init__(self, args):
        super(TodoListApp, self).__init__(args)
        db = inmemory.InMemoryDatabase("u1todo")
        self.store = TodoStore(db)
        self.ui = UIMainWindow()
        self.window = QtGui.QMainWindow()
        self.ui.setupUi(self.window)
        self.window.show()
        self.connect_events()
        self.item = None
        self.ui.deleteButton.setEnabled(False)
        self.exec_()

    def connect_events(self):
        """Hook up all the signal handlers."""
        self.connect(
            self.ui.updateButton, QtCore.SIGNAL("clicked()"), self.update)
        self.connect(
            self.ui.deleteButton, QtCore.SIGNAL("clicked()"), self.delete)
        self.connect(
            self.ui.listWidget, QtCore.SIGNAL("currentRowChanged(int)"),
            self.row_changed)

    def update(self):
        """Either add a new task or update an existing one."""
        text = self.ui.lineEdit.text()
        if not text:
            return
        if self.item is None:
            self.add_item(text)
        else:
            self.update_item(text)
        self.ui.lineEdit.clear()
        self.item = None

    def delete(self):
        """Delete a todo item."""
        item = self.ui.listWidget.takeItem(self.ui.listWidget.currentRow())
        self.store.delete_task(item.task)
        if self.ui.listWidget.count() == 0:
            self.ui.deleteButton.setEnabled(False)

    def add_item(self, text):
        """Add a new todo item."""
        task = self.store.new_task(title=unicode(text, 'utf-8'))
        item = UITask(task)
        self.ui.listWidget.addItem(item)
        self.ui.deleteButton.setEnabled(True)

    def update_item(self, text):
        """Edit an existing todo item."""
        self.item.task.title = unicode(text, 'utf-8')
        self.item.setText(text)
        self.store.save_task(self.item.task)

    def row_changed(self, index):
        """Edit item when row changes."""
        if index == -1:
            self.ui.lineEdit.clear()
            return
        self.edit_item(self.ui.listWidget.item(index))

    def edit_item(self, item):
        """Edit item in line edit box."""
        self.item = item
        self.ui.lineEdit.setText(item.task.title)


if __name__ == "__main__":
    app = TodoListApp(sys.argv)
