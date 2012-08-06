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

"""User interface for the cosas example application."""

from collections import defaultdict
from datetime import datetime
import os
import sys
from PyQt4 import QtGui, QtCore, uic

from cosas import TodoStore, get_database, extract_tags
import u1db
from u1db.errors import DatabaseDoesNotExist
from u1db.sync import Synchronizer
from u1db.remote.http_target import HTTPSyncTarget
from u1db.remote.http_database import HTTPDatabase
from ubuntuone.platform.credentials import CredentialsManagementTool

DONE_COLOR = QtGui.QColor(183, 183, 183)
NOT_DONE_COLOR = QtGui.QColor(0, 0, 0)
WHITE = QtGui.QColor(255, 255, 255)
TAG_COLORS = [
    (234, 153, 153),
    (249, 203, 156),
    (255, 229, 153),
    (182, 215, 168),
    (162, 196, 201),
    (164, 194, 244),
    (221, 126, 107),
    (159, 197, 232),
    (180, 167, 214),
    (213, 166, 189)]
U1_URL = 'https://u1db.one.ubuntu.com/~/cosas'
TIMEOUT = 0.5 * 60


class UITask(QtGui.QTreeWidgetItem):
    """Task list item."""

    def __init__(self, task, parent, store, font, main_window):
        super(UITask, self).__init__(parent)
        self.task = task
        # If the task is done, check off the list item.
        self.store = store
        self._bg_color = WHITE
        self._font = font
        self.main_window = main_window

    def set_color(self, color):
        self._bg_color = color

    def setData(self, column, role, value):
        if role == QtCore.Qt.CheckStateRole:
            if value == QtCore.Qt.Checked:
                self.task.done = True
            else:
                self.task.done = False
            self.store.save_task(self.task)
        if role == QtCore.Qt.EditRole:
            text = unicode(value.toString(), 'utf-8')
            if not text:
                # There was no text in the edit field so do nothing.
                return
            self.update_task_text(text)
        super(UITask, self).setData(column, role, value)

    def data(self, column, role):
        if role == QtCore.Qt.EditRole:
            return self.task.title
        if role == QtCore.Qt.DisplayRole:
            return self.task.title
        if role == QtCore.Qt.FontRole:
            font = self._font
            font.setStrikeOut(self.task.done)
            return font
        if role == QtCore.Qt.BackgroundRole:
            return self._bg_color
        if role == QtCore.Qt.ForegroundRole:
            return DONE_COLOR if self.task.done else NOT_DONE_COLOR
        if role == QtCore.Qt.CheckStateRole:
            return QtCore.Qt.Checked if self.task.done else QtCore.Qt.Unchecked
        return super(UITask, self).data(column, role)

    def update_task_text(self, text):
        """Edit an existing todo item."""
        # Change the task's title to the text in the edit field.
        self.task.title = text
        # Record the current tags.
        old_tags = set(self.task.tags) if self.task.tags else set([])
        # Extract the new tags from the new text.
        new_tags = set(extract_tags(text))
        # Check if the tag filter buttons need updating.
        self.main_window.update_tags(self, old_tags, new_tags)
        # Set the tags on the task.
        self.task.tags = list(new_tags)
        # Save the changed task to the database.
        self.store.save_task(self.task)


class TaskDelegate(QtGui.QStyledItemDelegate):
    """Delegate for rendering tasks."""

    pen = QtGui.QPen(QtGui.QColor(102, 102, 102), 2, style=QtCore.Qt.DotLine)

    def paint(self, painter, option, index):
        # Save current state of painter before we modify anything.
        painter.save()
        painter.setPen(self.pen)
        painter.drawRect(option.rect)
        # Return painter to original stats.
        painter.restore()
        super(TaskDelegate, self).paint(painter, option, index)


class Sync(QtGui.QDialog):

    def __init__(self, other):
        super(Sync, self).__init__()
        uifile = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), 'sync.ui')
        uic.loadUi(uifile, self)
        self.other = other
        if other.auto_sync:
            self.auto_sync.setChecked(True)
        if other.sync_target == U1_URL:
            self.u1_radio.setChecked(True)
            self.url_radio.setChecked(False)
        else:
            self.url_radio.setChecked(True)
            self.u1_radio.setChecked(False)
            self.url_edit.setText(other.sync_target)
        self.connect_events()

    def connect_events(self):
        """Hook up all the signal handlers."""
        self.sync_button.clicked.connect(self.synchronize)
        self.u1_radio.toggled.connect(self.toggle_u1)
        self.url_radio.toggled.connect(self.toggle_url)
        self.auto_sync.toggled.connect(self.toggle_sync)
        self.url_edit.editingFinished.connect(self.url_changed)

    def enable_button(self, _=None):
        self.sync_button.setEnabled(True)

    def synchronize(self):
        self.sync_button.setEnabled(False)
        self.other.synchronize(self.enable_button)
        self.last_synced.setText(
            '<span style="color:green">%s</span>' % (datetime.now(),))

    def toggle_u1(self, value):
        if value:
            self.other.sync_target = U1_URL
        else:
            text = unicode(self.url_edit.text(), 'utf-8')
            if not text:
                # There was no text in the edit field so do nothing.
                self.other.sync_target = None
                return
        self.other.sync_target = text

    def toggle_url(self, value):
        if value:
            text = unicode(self.url_edit.text(), 'utf-8')
            if not text:
                # There was no text in the edit field so do nothing.
                self.other.sync_target = None
                return
        else:
            self.other.sync_target = U1_URL
        self.other.sync_target = text

    def toggle_sync(self, value):
        self.other.auto_sync = value
        if value:
            self.other.start_auto_sync()
        else:
            self.other.stop_auto_sync()

    def url_changed(self):
        if not self.url_radio.isChecked():
            return
        text = unicode(self.url_edit.text(), 'utf-8')
        if not text:
            # There was no text in the edit field so do nothing.
            self.other.sync_target = None
            return
        self.other.sync_target = text


class Main(QtGui.QMainWindow):
    """Main window of our application."""

    def __init__(self, in_memory=False):
        super(Main, self).__init__()
        # Dynamically load the ui file generated by QtDesigner.
        uifile = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), 'cosas.ui')
        uic.loadUi(uifile, self)
        self.buttons_frame.hide()
        # hook up the signals to the signal handlers.
        self.connect_events()
        # Load the cosas database.
        db = get_database()
        # And wrap it in a TodoStore object.
        self.store = TodoStore(db)
        # create or update the indexes if they are not up-to-date
        self.store.initialize_db()
        # hook up the delegate
        self.todo_list.setItemDelegate(TaskDelegate())
        # Initialize some variables we will use to keep track of the tags.
        self._tag_docs = defaultdict(list)
        self._tag_buttons = {}
        self._tag_filter = []
        self._tag_colors = {}
        # A list of colors to give differently tagged items different colors.
        self.colors = TAG_COLORS[:]
        # Get all the tasks in the database, and add them to the UI.
        for task in self.store.get_all_tasks():
            self.add_task(task)
        self.title_edit.clear()
        # Give the edit field focus.
        self.title_edit.setFocus()
        self.editing = False
        self.last_synced = None
        self.sync_target = U1_URL
        self.auto_sync = False
        self._scheduled = None

    def update_status_bar(self, message):
        self.statusBar.showMessage(message)

    def keyPressEvent(self, event):
        if event.key() == QtCore.Qt.Key_Delete:
            self.delete()
            return
        if event.key() == QtCore.Qt.Key_Return:
            if not self.editing:
                self.editing = True
                self.todo_list.openPersistentEditor(
                    self.todo_list.currentItem())
                return
            else:
                self.todo_list.closePersistentEditor(
                    self.todo_list.currentItem())
                self.editing = False
        super(Main, self).keyPressEvent(event)

    def get_tag_color(self):
        """Get a color number to use for a new tag."""
        # Remove a color from the list of available ones and return it.
        if not self.colors:
            return WHITE
        return self.colors.pop(0)

    def connect_events(self):
        """Hook up all the signal handlers."""
        # On enter, save the task that was being edited.
        self.title_edit.returnPressed.connect(self.update)
        self.action_synchronize.triggered.connect(self.open_sync_window)
        self.buttons_toggle.clicked.connect(self.show_buttons)

    def open_sync_window(self):
        window = Sync(self)
        window.exec_()

    def show_buttons(self):
        """Show the frame with the tag buttons."""
        self.buttons_toggle.clicked.disconnect(self.show_buttons)
        self.buttons_frame.show()
        self.buttons_toggle.clicked.connect(self.hide_buttons)

    def hide_buttons(self):
        """Show the frame with the tag buttons."""
        self.buttons_toggle.clicked.disconnect(self.hide_buttons)
        self.buttons_frame.hide()
        self.buttons_toggle.clicked.connect(self.show_buttons)

    def refresh_filter(self):
        """Remove all tasks, and show only those that satisfy the new filter.

        """
        # Remove everything from the list.
        while self.todo_list.topLevelItemCount():
            self.todo_list.takeTopLevelItem(0)
        # Get the filtered tasks from the database.
        for task in self.store.get_tasks_by_tags(self._tag_filter):
            # Add them to the UI.
            self.add_task(task)
        # Clear the current selection.
        self.todo_list.setCurrentItem(None)
        self.title_edit.clear()
        self.item = None

    def update(self):
        """Either add a new task or update an existing one."""
        text = unicode(self.title_edit.text(), 'utf-8')
        if not text:
            # There was no text in the edit field so do nothing.
            return
        # No task was selected, so add a new one.
        task = self.store.new_task(text, tags=extract_tags(text))
        self.add_task(task)
        # Clear the current selection.
        self.title_edit.clear()

    def delete(self):
        """Delete a todo item."""
        # Delete the item from the database.
        index = self.todo_list.indexFromItem(self.todo_list.currentItem())
        item = self.todo_list.takeTopLevelItem(index.row())
        if item is None:
            return
        self.store.delete_task(item.task)
        # Clear the current selection.
        self.item = None

    def add_task(self, task):
        """Add a new todo item."""
        # Wrap the task in a UITask object.
        item = UITask(
            task, self.todo_list, self.store, self.todo_list.font(), self)
        item.setFlags(item.flags() | QtCore.Qt.ItemIsEditable)
        self.todo_list.addTopLevelItem(item)
        if not task.tags:
            return
        # If the task has tags, we add them as filter buttons to the UI, if
        # they are new.
        for tag in task.tags:
            self.add_tag(task.doc_id, tag)
        if task.tags:
            item.set_color(self._tag_colors[task.tags[0]]['qcolor'])
        else:
            item.set_color(WHITE)

    def add_tag(self, doc_id, tag):
        """Create a link between the task with id doc_id and the tag, and
        add a new button for tag if it was not already there.

        """
        # Add the task id to the list of document ids associated with this tag.
        self._tag_docs[tag].append(doc_id)
        # If the list has more than one element the tag button was already
        # present.
        if len(self._tag_docs[tag]) > 1:
            return
        # Add a tag filter button for this tag to the UI.
        button = QtGui.QPushButton(tag)
        color = self.get_tag_color()
        qcolor = QtGui.QColor(*color)
        self._tag_colors[tag] = {
            'color_tuple': color,
            'qcolor': qcolor}
        button.setStyleSheet('background-color: rgb(%d, %d, %d)' % color)
        button._todo_tag = tag
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
                self._tag_filter.append(button._todo_tag)
            else:
                # Remove the tag from the current filter.
                self._tag_filter.remove(button._todo_tag)
            # Apply the new filter.
            self.refresh_filter()

        # Attach the handler to the button's clicked signal.
        button.clicked.connect(filter_toggle)
        # Get the position where the button needs to be inserted. (We keep them
        # sorted alphabetically by the text of the tag.
        index = sorted(self._tag_buttons.keys()).index(tag)
        # And add the button to the UI.
        self.buttons_layout.insertWidget(index, button)

    def remove_tag(self, doc_id, tag):
        """Remove the link between the task with id doc_id and the tag, and
        remove the button for tag if it no longer has any tasks associated with
        it.

        """
        # Remove the task id from the list of document ids associated with this
        # tag.
        self._tag_docs[tag].remove(doc_id)
        # If the list is not empty, we do not remove the button, because there
        # are still tasks that have this tag.
        if self._tag_docs[tag]:
            return
        # Look up the button.
        button = self._tag_buttons[tag]
        # Remove it from the ui.
        button.hide()
        self.buttons_layout.removeWidget(button)
        # And remove the reference.
        del self._tag_buttons[tag]

    def update_tags(self, item, old_tags, new_tags):
        """Process any changed tags for this item."""
        # Process all removed tags.
        for tag in old_tags - new_tags:
            self.remove_tag(item.task.doc_id, tag)
        # Process all tags newly added.
        for tag in new_tags - old_tags:
            self.add_tag(item.task.doc_id, tag)
        if new_tags:
            item.set_color(self._tag_colors[list(new_tags)[0]]['qcolor'])
            return
        item.set_color(WHITE)

    def get_ubuntuone_credentials(self):
        cmt = CredentialsManagementTool()
        return cmt.find_credentials()

    def synchronize(self, finalize):
        if self.sync_target == 'https://u1db.one.ubuntu.com/~/cosas':
            d = self.get_ubuntuone_credentials()
            d.addCallback(self._synchronize)
            d.addCallback(finalize)
        else:
            # TODO: add ui for entering creds for non u1 servers.
            self._synchronize()
            finalize()

    def start_auto_sync(self, _=None):
        self._scheduled = reactor.callLater(
            TIMEOUT, self.synchronize, self.start_auto_sync)

    def stop_auto_sync(self):
        if self._scheduled:
            self._scheduled.cancel()
            self._scheduled = None

    def _synchronize(self, creds=None):
        target = self.sync_target
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
        self.update_status_bar("last synced: %s" % (datetime.now(),))


if __name__ == "__main__":
    # TODO: Unfortunately, to be able to use ubuntuone.platform.credentials on
    # linux, we now depend on dbus. :(
    from dbus.mainloop.qt import DBusQtMainLoop
    main_loop = DBusQtMainLoop(set_as_default=True)
    app = QtGui.QApplication(sys.argv)
    import qt4reactor
    qt4reactor.install()
    from twisted.internet import reactor
    main = Main()
    main.show()
    app.exec_()
