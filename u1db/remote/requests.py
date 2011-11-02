# Copyright 2011 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 3, as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Define the requests that can be made."""

from u1db import __version__ as _u1db_version


class ServerState(object):
    """Passed to a Request when it is instantiated.

    This is used to track server-side state, such as working-directory, open
    databases, etc.
    """

    def __init__(self):
        self._workingdir = None

    def set_workingdir(self, path):
        self._workingdir = path

    def _relpath(self, relpath):
        # Note: We don't want to allow absolute paths here, because we
        #       don't want to expose the filesystem. We should also check that
        #       relpath doesn't have '..' in it, etc.
        return self._workingdir + '/' + relpath

    def open_database(self, path):
        """Open a database at the given location."""
        from u1db.backends import sqlite_backend
        full_path = self._relpath(path)
        return sqlite_backend.SQLiteDatabase.open_database(full_path)


class RPCRequest(object):
    """Base class for request instances.

    Children of this will be instantiated when a request for them comes in.

    :cvar name: The name of the request, this is the lookup name that will be
        used to find the factory, so it must be unique.
    :ivar response: This gets set to the response that should be sent back to
        the caller. If it is None, then no response is ready. If it is not
        None, then it should be an instance of RPCSuccessfulResponse or
        RPCFailureResponse.
    """

    requests = {}
    name = None

    @classmethod
    def register(cls):
        RPCRequest.requests[cls.name] = cls

    @classmethod
    def unregister(cls):
        if cls.name in RPCRequest.requests:
            RPCRequest.requests.pop(cls.name)

    def __init__(self, state, responder):
        # This will get instantiated once we receive the "header" portion of
        # the request.
        self.state = state
        self.responder = responder

    def handle_args(self, **kwargs):
        """This will be called when a request passes an 'args' section.

        Child classes should implement this to handle arguments that are
        passed. Note that parameters are passed as **kwargs, so each argument
        is named.
        """
        raise NotImplementedError(self.handle_args)

    def handle_end(self):
        """This will be called when a request sends the end indicator."""
        # The default implementation is to just ignore the end.


class RPCServerVersion(RPCRequest):
    """Return the version of the server."""

    name = 'version'

    def __init__(self, state, responder):
        super(RPCServerVersion, self).__init__(state, responder)

    def handle_end(self):
        self.responder.send_response(version=_u1db_version)

RPCServerVersion.register()


class SyncTargetRPC(RPCRequest):
    """See u1db.SyncTarget

    This is a common base class for RPCs that represent SyncTarget functions.
    """

    def _get_sync_target(self, path):
        assert path.startswith('/')
        path = path.lstrip('/')
        self.db = self.state.open_database(path)
        self.target = self.db.get_sync_target()

    def _result(self, **kwargs):
        self.responder.send_response(**kwargs)
        # If we have a result, then we can close this db connection.
        self._close()

    def _close(self):
        self.target = None
        self.db = None


class RPCGetSyncInfo(SyncTargetRPC):

    name = "get_sync_info"

    def handle_args(self, path, other_replica_uid):
        self._get_sync_target(path)
        result = self.target.get_sync_info(other_replica_uid)
        self._result(this_replica_uid=result[0],
                     this_replica_generation=result[1],
                     other_replica_uid=other_replica_uid,
                     other_replica_generation=result[2])

RPCGetSyncInfo.register()


class RPCRecordSyncInfo(SyncTargetRPC):

    name = "record_sync_info"

    def handle_args(self, path, other_replica_uid, other_replica_generation):
        self._get_sync_target(path)
        self.target.record_sync_info(other_replica_uid,
                                     other_replica_generation)
        self._result()

RPCRecordSyncInfo.register()


class RPCSyncExchange(SyncTargetRPC):

    name = "sync_exchange"

    # Implements the same logic as LocalSyncTarget.sync_exchange

    def handle_args(self, path, from_replica_uid, from_replica_generation,
                    last_known_generation):
        self._get_sync_target(path)
        self.from_replica_uid = from_replica_uid
        self.from_replica_generation = from_replica_generation
        self.last_known_generation = last_known_generation
        self.sync_exch = self.target.get_sync_exchange()

    def handle_stream_entry(self, entry):
        self.sync_exch.insert_doc_from_source(entry['doc_id'], entry['doc_rev'],
                                              entry['doc'])

    def handle_end(self):
        def send_doc(doc_id, doc_rev, doc):
            entry = dict(doc_id=doc_id, doc_rev=doc_rev, doc=doc)
            self.responder.stream_entry(entry)
        new_gen = self.sync_exch.find_docs_to_return(self.last_known_generation)
        self.responder.start_response(other_new_generation=new_gen)
        new_gen = self.sync_exch.return_docs_and_record_sync(
                                                  self.from_replica_uid,
                                                  self.from_replica_generation,
                                                  send_doc)
        self.responder.finish_response()
        self._close()

RPCSyncExchange.register()
