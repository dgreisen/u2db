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

    def __init__(self):
        # TODO: We'll need some amount of server-state context to pass into all
        #       of these requests (such as WorkingDirectory, or something).
        # This will get instantiated once we receive the "header" portion of
        # the request.
        self.response = None

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


class RPCResponse(object):
    """Base class for responses to RPC requests."""


class RPCSuccessfulResponse(RPCResponse):
    """Used to indicate that the request was successful.
    """

    status = 'success'

    def __init__(self, request_name, **response_kwargs):
        """Create a new Successful Response.

        Pass in whatever arguments you want to return to the client.
        """
        self.request_name = request_name
        self.response_kwargs = response_kwargs


class RPCFailureResponse(RPCResponse):
    """Used to indicate there was a failure processing the request."""

    status = 'fail'



class RPCServerVersion(RPCRequest):
    """Return the version of the server."""

    name = 'version'

    def __init__(self):
        self.response = RPCSuccessfulResponse(self.name, version=_u1db_version)


RPCServerVersion.register()
