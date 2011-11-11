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

"""HTTP Application exposing U1DB."""
import httplib
import json
import urlparse


class DocResource(object):
    """Document resource."""

    def __init__(self, dbname, id, state, responder):
        self.id = id
        self.responder = responder
        self.db = state.open_database(dbname)

    def put(self, body, args):
        doc_rev = self.db.put_doc(self.id, None, body)
        self.responder.send_response(rev=doc_rev) # xxx some other 20x status


class SyncResource(object):
    """Sync endpoint resource."""

    def __init__(self, dbname, from_replica_uid, state, responder):
        self.from_replica_uid = from_replica_uid
        self.responder = responder
        self.target = state.open_database(dbname).get_sync_target()

    def get(self, args):
        result = self.target.get_sync_info(self.from_replica_uid)
        self.responder.send_response(this_replica_uid=result[0],
                                     this_replica_generation=result[1],
                                     other_replica_uid=self.from_replica_uid,
                                     other_replica_generation=result[2])

    def put(self, body, args): # xxx ask for the upper layers to unserialize
        data = json.loads(body)
        self.target.record_sync_info(self.from_replica_uid,
                                     data['generation'])
        self.responder.send_response(ok=True)

    # Implements the same logic as LocalSyncTarget.sync_exchange

    def post_args(self, args):
        self.from_replica_generation = args['from_replica_generation']
        self.last_known_generation = args['last_known_generation']
        self.sync_exch = self.target.get_sync_exchange()

    def post_stream_entry(self, entry):
        entry = json.loads(entry)
        self.sync_exch.insert_doc_from_source(entry['id'], entry['rev'],
                                              entry['doc'])

    def post_end(self):
        def send_doc(doc_id, doc_rev, doc):
            entry = dict(id=doc_id, rev=doc_rev, doc=doc)
            self.responder.stream_entry(entry)
        new_gen = self.sync_exch.find_docs_to_return(self.last_known_generation)
        self.responder.content_type = 'application/x-u1db-multi-json'
        self.responder.start_response(new_generation=new_gen)
        new_gen = self.sync_exch.return_docs_and_record_sync(
                                                  self.from_replica_uid,
                                                  self.from_replica_generation,
                                                  send_doc)
        self.responder.finish_response()


OK = 200

class HTTPResponder(object):
    """Encode responses from the server back to the client."""

    # a multi document response will put args and documents
    # each on one line of the response body

    def __init__(self, start_response):
        self._started = False
        self.sent_response = False
        self._start_response = start_response
        self._write = None
        self.content_type = 'application/json'

    def start_response(self, status=OK, **kwargs):
        """start sending response: header and args."""
        if self._started:
            return
        self._started = True
        status_text = httplib.responses[status]
        self._write = self._start_response('%d %s' % (status, status_text),
                                         [('content-type', self.content_type),
                                          ('cache-control', 'no-cache')])
        # xxx version in headers
        if kwargs:
            self._write(json.dumps(kwargs)+"\r\n")

    def finish_response(self):
        """finish sending response."""
        self.sent_response = True

    def send_response(self, status=OK, **kwargs):
        """send and finish response in one go."""
        self.start_response(status, **kwargs)
        self.finish_response()

    def stream_entry(self, entry):
        "send stream entry as part of the response."
        assert self._started
        self._write(json.dumps(entry)+"\r\n")


class BadRequest(Exception):
    """Bad request."""


class HTTPInvocationByMethodWithBody(object):
    """Invoke methods on a resource."""

    def __init__(self, resource, environ):
        self.resource = resource
        self.environ = environ

    def _lookup(self, method):
        try:
            return getattr(self.resource, method)
        except AttributeError:
            raise BadRequest()

    def __call__(self):
        args = urlparse.parse_qsl(self.environ['QUERY_STRING'],
                                  strict_parsing=False)
        try:
            args = dict((k.decode('utf-8'), v.decode('utf-8')) for k,v in args)
        except ValueError:
            raise BadRequest()
        method = self.environ['REQUEST_METHOD'].lower()
        if method in ('get', 'delete'):
            meth = self._lookup(method)
            return meth(args)
        else:
            content_type = self.environ.get('CONTENT_TYPE')
            if content_type == 'application/json':
                meth = self._lookup(method)
                body = self.environ['wsgi.input'].read()
                return meth(body, args)
            elif content_type == 'application/x-u1db-multi-json':
                meth_args = self._lookup('%s_args' % method)
                meth_entry = self._lookup('%s_stream_entry' % method)
                meth_end = self._lookup('%s_end' % method)
                body_readline = self.environ['wsgi.input'].readline
                args.update(json.loads(body_readline()))
                meth_args(args)
                while True:
                    line = body_readline()
                    if not line:
                        break
                    entry = line.strip()
                    meth_entry(entry)
                return meth_end()
            else:
                raise BadRequest()


class HTTPApp(object):

    def __init__(self, state):
        self.state = state

    def _lookup_resource(self, environ, responder):
        # xxx proper dispatch logic
        parts = environ['PATH_INFO'].split('/')
        if len(parts) == 4 and parts[2] == 'doc':
            resource = DocResource(parts[1], parts[3], self.state, responder)
        elif len(parts) == 4 and parts[2] == 'sync-from':
            resource = SyncResource(parts[1], parts[3], self.state, responder)
        else:
            raise BadRequest()
        return resource

    def __call__(self, environ, start_response):
        responder = HTTPResponder(start_response)
        try:
            resource = self._lookup_resource(environ, responder)
            HTTPInvocationByMethodWithBody(resource, environ)()
        except BadRequest:
            responder.send_response(400)
        return []
