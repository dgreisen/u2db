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

"""HTTPDabase to access a remote db over the HTTP API."""

import httplib
import simplejson
import urlparse
import urllib

from u1db import (
    Database
    )


class HTTPClientBase(object):
    """Base class to make requests to a remote HTTP server."""

    def __init__(self, url):
        self._url = urlparse.urlsplit(url)
        self._conn = None

    def _ensure_connection(self):
        if self._conn is not None:
            return
        self._conn = httplib.HTTPConnection(self._url.hostname,
                                              self._url.port)

    # xxx retry mechanism?

    def _request(self, method, url_parts, params=None, body=None,
                                                       content_type=None):
        self._ensure_connection()
        url_query = '/'.join([self._url.path] + url_parts)
        if params:
            url_query += ('?' +
                      urllib.urlencode(dict((unicode(v).encode('utf-8'),
                                             unicode(k).encode('utf-8'))
                                            for v, k in params.items())))
        if body is not None and not isinstance(body, basestring):
            body = simplejson.dumps(body)
            content_type = 'application/json'
        headers = {}
        if content_type:
            headers['content-type'] = content_type
        self._conn.request(method, url_query, body, headers)
        resp = self._conn.getresponse()
        if resp.status in (200, 201):
            return resp.read(), dict(resp.getheaders())
        else:
            # xxx
            raise Exception(resp.status)

    def _request_json(self, method, url_parts, params=None, body=None,
                                                            content_type=None):
        res, headers = self._request(method, url_parts, params, body,
                                     content_type)
        return simplejson.loads(res), headers


class HTTPDatabase(HTTPClientBase, Database):
    """Implement the Database API to a remote HTTP server."""

    def put_doc(self, doc_id, old_doc_rev, doc):
        params = {}
        if old_doc_rev is not None:
            params['old_rev'] = old_doc_rev
        res, headers = self._request_json('PUT', ['doc', doc_id], params,
                                          doc, 'application/json')
        return res['rev']


    def get_doc(self, doc_id):
        res, headers = self._request('GET', ['doc', doc_id])
        doc_rev = headers['x-u1db-rev']
        has_conflicts = simplejson.loads(headers['x-u1db-has-conflicts'])
        return doc_rev, res, has_conflicts
