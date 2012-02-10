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

"""Base class to make requests to a remote HTTP server."""

import httplib
from oauth import oauth
import simplejson
import urlparse
import urllib

from u1db import (
    errors,
    )
from u1db.remote import (
    http_errors,
    )


class HTTPClientBase(object):
    """Base class to make requests to a remote HTTP server."""

    def __init__(self, url):
        self._url = urlparse.urlsplit(url)
        self._conn = None
        self._oauth_creds = None

    oauth_signature_method = oauth.OAuthSignatureMethod_HMAC_SHA1()

    def set_oauth_credentials(self, consumer_key, consumer_secret,
                              token_key, token_secret):
        self._oauth_creds = (oauth.OAuthConsumer(consumer_key, consumer_secret),
                             oauth.OAuthToken(token_key, token_secret))

    def _ensure_connection(self):
        if self._conn is not None:
            return
        self._conn = httplib.HTTPConnection(self._url.hostname,
                                              self._url.port)

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # xxx retry mechanism?

    def _response(self):
        resp = self._conn.getresponse()
        body = resp.read()
        headers = dict(resp.getheaders())
        if resp.status in (200, 201):
            return body, headers
        elif resp.status in http_errors.ERROR_STATUSES:
            try:
                respdic = simplejson.loads(body)
            except ValueError:
                pass
            else:
                descr = respdic.get("error")
                exc_cls = errors.wire_description_to_exc.get(descr)
                if exc_cls is not None:
                    message = respdic.get("message")
                    raise exc_cls(message)
        raise errors.HTTPError(resp.status, body, headers)

    def _request(self, method, url_parts, params=None, body=None,
                                                       content_type=None):
        self._ensure_connection()
        url_query = self._url.path
        if url_parts:
            if not url_query.endswith('/'):
                url_query += '/'
            url_query += '/'.join(urllib.quote(part, safe='')
                                  for part in url_parts)
        if params:
            params = dict((unicode(v).encode('utf-8'),
                           unicode(k).encode('utf-8'))
                                                   for v, k in params.items())
            url_query += ('?' + urllib.urlencode(params))
        if body is not None and not isinstance(body, basestring):
            body = simplejson.dumps(body)
            content_type = 'application/json'
        headers = {}
        if content_type:
            headers['content-type'] = content_type
        if self._oauth_creds:
            consumer, token = self._oauth_creds
            full_url = "%s://%s%s" % (self._url.scheme, self._url.netloc,
                                      url_query)
            oauth_req = oauth.OAuthRequest.from_consumer_and_token(
                consumer, token,
                http_method=method,
                parameters=params,
                http_url=full_url
                )
            oauth_req.sign_request(self.oauth_signature_method, consumer, token)
            # Add Authorization: OAuth ...
            headers.update(oauth_req.to_header())
        self._conn.request(method, url_query, body, headers)
        return self._response()

    def _request_json(self, method, url_parts, params=None, body=None,
                                                            content_type=None):
        res, headers = self._request(method, url_parts, params, body,
                                     content_type)
        return simplejson.loads(res), headers
