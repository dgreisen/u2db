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
"""U1DB Basic Auth authorisation WSGI middleware."""
import httplib
try:
    import simplejson as json
except ImportError:
    import json  # noqa
from wsgiref.util import shift_path_info


class BasicAuthMiddleware(object):
    """U1DB Basic Auth Authorisation WSGI middleware."""

    def __init__(self, app, base_url):
        self.app = app
        self.base_url = base_url

    def _error(self, start_response, status, description, message=None):
        start_response("%d %s" % (status, httplib.responses[status]),
                       [('content-type', 'application/json')])
        err = {"error": description}
        if message:
            err['message'] = message
        return [json.dumps(err)]

    def __call__(self, environ, start_response):
        if not environ['PATH_INFO'].startswith('/~/'):
            return self._error(start_response, 400, "bad request")
        headers = {}
        auth = environ.get('HTTP_AUTHORIZATION')
        if auth:
            headers['Authorization'] = auth
        else:
            return self._error(start_response, 401, "unauthorized",
                               "Missing Basic Authentication.")
        scheme, encoded = auth.split(None, 1)
        if scheme.lower() != 'basic':
            return self._error(
                start_response, 401, "unauthorized",
                "Missing Basic Authentication")
        user, password = encoded.decode('base64').split(':', 1)
        if not self.verify(user, password):
            return self._error(
                start_response, 401, "unauthorized",
                "Incorrect password or login.")
        del environ['HTTP_AUTHORIZATION']
        environ['user_id'] = user
        shift_path_info(environ)
        return self.app(environ, start_response)

    def verify_user(self, username, password):
        raise NotImplementedError(self.verify_user)
