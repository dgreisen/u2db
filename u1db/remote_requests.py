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


class RPCRequest(object):
    """Base class for request instances."""

    requests = {}

    @classmethod
    def register(cls):
        RPCRequest.requests[cls.name] = cls

    @classmethod
    def unregister(cls):
        if cls.name in RPCRequest.requests:
            RPCRequest.requests.pop(cls.name)

    @staticmethod
    def lookup(request_name):
        return RPCRequest.requests.get(request_name, None)


class RPCResponse(object):
    """Base class for responses to RPC requests."""


class RPCSuccessfulResponse(RPCResponse):
    pass


class RPCFailureResponse(RPCResponse):
    pass


class RPCServerVersion(RPCRequest):
    """Return the version of the server."""

    name = 'version'

RPCServerVersion.register()
