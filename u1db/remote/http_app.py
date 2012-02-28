# Copyright 2011-2012 Canonical Ltd.
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

"""HTTP Application exposing U1DB."""

import functools
import httplib
import inspect
import simplejson
import sys
import urlparse

import routes.mapper

from u1db import (
    __version__ as _u1db_version,
    Document,
    errors,
    DBNAME_CONSTRAINTS,
    )
from u1db.remote import (
    http_errors,
    utils,
    )


class _FencedReader(object):
    """Read and get lines from a file but not past a given length."""

    MAXCHUNK = 8192

    # xxx do checks for maximum admissible total size
    # and maximum admissible line size

    def __init__(self, rfile, total):
        self.rfile = rfile
        self.remaining = total
        self._kept = None

    def read_chunk(self, atmost):
        if self._kept is not None:
            # ignore atmost, kept data should be a subchunk anyway
            kept, self._kept = self._kept, None
            return kept
        if self.remaining == 0:
            return ''
        data = self.rfile.read(min(self.remaining, atmost))
        self.remaining -= len(data)
        return data

    def getline(self):
        line_parts = []
        while True:
            chunk = self.read_chunk(self.MAXCHUNK)
            if chunk == '':
                break
            nl = chunk.find("\n")
            if nl != -1:
                line_parts.append(chunk[:nl + 1])
                rest = chunk[nl + 1:]
                self._kept = rest or None
                break
            else:
                line_parts.append(chunk)
        return ''.join(line_parts)


class BadRequest(Exception):
    """Bad request."""


def http_method(**control):
    """Decoration for handling of query arguments and content for a HTTP method.

       args and content here are the query arguments and body of the incoming
       HTTP requests.

       Match query arguments to python method arguments:
           w = http_method()(f)
           w(self, args, content) => args["content"]=content;
                                     f(self, **args)

       JSON deserialize content to arguments:
           w = http_method(content_as_args=True,...)(f)
           w(self, args, content) => args.update(simplejson.loads(content));
                                     f(self, **args)

       Support conversions (e.g int):
           w = http_method(Arg=Conv,...)(f)
           w(self, args, content) => args["Arg"]=Conv(args["Arg"]);
                                     f(self, **args)

       Enforce no use of query arguments:
           w = http_method(no_query=True,...)(f)
           w(self, args, content) raises BadRequest if args is not empty

       Argument mismatches, deserialisation failures produce BadRequest.
    """
    content_as_args = control.pop('content_as_args', False)
    no_query = control.pop('no_query', False)
    conversions = control.items()
    def wrap(f):
        argspec = inspect.getargspec(f)
        assert argspec.args[0] == "self"
        nargs = len(argspec.args)
        ndefaults = len(argspec.defaults or ())
        required_args = set(argspec.args[1:nargs - ndefaults])
        all_args = set(argspec.args)
        @functools.wraps(f)
        def wrapper(self, args, content):
            if no_query and args:
                raise BadRequest()
            if content is not None:
                if content_as_args:
                    try:
                        args.update(simplejson.loads(content))
                    except ValueError:
                        raise BadRequest()
                else:
                    args["content"] = content
            if not (required_args <= set(args) <= all_args):
                raise BadRequest()
            for name, conv in conversions:
                if name not in args:
                    continue
                try:
                    args[name] = conv(args[name])
                except ValueError:
                    raise BadRequest()
            return f(self, **args)
        return wrapper
    return wrap


class URLToResource(object):
    """Mappings from URLs to resources."""

    def __init__(self):
        self._map = routes.mapper.Mapper(controller_scan=None)

    def register(self, resource_cls):
        # register
        self._map.connect(None, resource_cls.url_pattern,
                          resource_cls=resource_cls,
                          requirements={"dbname": DBNAME_CONSTRAINTS})
        self._map.create_regs()
        return resource_cls

    def match(self, path):
        params = self._map.match(path)
        if params is None:
            return None, None
        resource_cls = params.pop('resource_cls')
        return resource_cls, params

url_to_resource = URLToResource()


@url_to_resource.register
class GlobalResource(object):
    """Global (root) resource."""

    url_pattern = "/"

    def __init__(self, state, responder):
        self.responder = responder

    @http_method()
    def get(self):
        self.responder.send_response_json(version=_u1db_version)


@url_to_resource.register
class DatabaseResource(object):
    """Database resource."""

    url_pattern = "/{dbname}"

    def __init__(self, dbname, state, responder):
        self.dbname = dbname
        self.state = state
        self.responder = responder

    @http_method()
    def get(self):
        self.state.check_database(self.dbname)
        self.responder.send_response_json(200)

    @http_method(content_as_args=True)
    def put(self):
        self.state.ensure_database(self.dbname)
        self.responder.send_response_json(200, ok=True)


@url_to_resource.register
class DocResource(object):
    """Document resource."""

    url_pattern = "/{dbname}/doc/{id:.*}"

    def __init__(self, dbname, id, state, responder):
        self.id = id
        self.responder = responder
        self.db = state.open_database(dbname)

    @http_method(old_rev=str)
    def put(self, content, old_rev=None):
        doc = Document(self.id, old_rev, content)
        doc_rev = self.db.put_doc(doc)
        if old_rev is None:
            status = 201  # created
        else:
            status = 200
        self.responder.send_response_json(status, rev=doc_rev)

    @http_method(old_rev=str)
    def delete(self, old_rev=None):
        doc = Document(self.id, old_rev, None)
        self.db.delete_doc(doc)
        self.responder.send_response_json(200, rev=doc.rev)

    @http_method()
    def get(self):
        doc = self.db.get_doc(self.id)
        if doc is None:
            wire_descr = errors.DocumentDoesNotExist.wire_description
            self.responder.send_response_json(
                http_errors.wire_description_to_status[wire_descr],
                error=wire_descr,
                headers={
                    'x-u1db-rev': '',
                    'x-u1db-has-conflicts': 'false'
                    })
            return
        headers = {
            'x-u1db-rev': doc.rev,
            'x-u1db-has-conflicts': simplejson.dumps(doc.has_conflicts)
            }
        if doc.content is None:
            self.responder.send_response_json(
               http_errors.wire_description_to_status[errors.DOCUMENT_DELETED],
               error=errors.DOCUMENT_DELETED,
               headers=headers)
        else:
            self.responder.send_response_content(doc.content, headers=headers)


@url_to_resource.register
class SyncResource(object):
    """Sync endpoint resource."""

    url_pattern = "/{dbname}/sync-from/{source_replica_uid}"

    def __init__(self, dbname, source_replica_uid, state, responder):
        self.source_replica_uid = source_replica_uid
        self.responder = responder
        self.target = state.open_database(dbname).get_sync_target()

    @http_method()
    def get(self):
        result = self.target.get_sync_info(self.source_replica_uid)
        self.responder.send_response_json(target_replica_uid=result[0],
                                     target_replica_generation=result[1],
                                     source_replica_uid=self.source_replica_uid,
                                     source_replica_generation=result[2])

    @http_method(generation=int,
                 content_as_args=True, no_query=True)
    def put(self, generation):
        self.target.record_sync_info(self.source_replica_uid, generation)
        self.responder.send_response_json(ok=True)

    # Implements the same logic as LocalSyncTarget.sync_exchange

    @http_method(last_known_generation=int,
                 content_as_args=True)
    def post_args(self, last_known_generation):
        self.last_known_generation = last_known_generation
        self.sync_exch = self.target._get_sync_exchange(self.source_replica_uid)

    @http_method(content_as_args=True)
    def post_stream_entry(self, id, rev, content, gen):
        doc = Document(id, rev, content)
        self.sync_exch.insert_doc_from_source(doc, gen)

    def post_end(self):
        def send_doc(doc, gen):
            entry = dict(id=doc.doc_id, rev=doc.rev, content=doc.content,
                         gen=gen)
            self.responder.stream_entry(entry)
        new_gen = self.sync_exch.find_changes_to_return(
                                                    self.last_known_generation)
        self.responder.content_type = 'application/x-u1db-sync-stream'
        self.responder.start_response(200)
        self.responder.start_stream(),
        self.responder.stream_entry({"new_generation": new_gen})
        new_gen = self.sync_exch.return_docs(send_doc)
        self.responder.end_stream()
        self.responder.finish_response()


class HTTPResponder(object):
    """Encode responses from the server back to the client."""

    # a multi document response will put args and documents
    # each on one line of the response body

    def __init__(self, start_response):
        self._started = False
        self._stream_state = -1
        self._no_initial_obj = True
        self.sent_response = False
        self._start_response = start_response
        self._write = None
        self.content_type = 'application/json'
        self.content = []

    def start_response(self, status, obj_dic=None, headers={}):
        """start sending response with optional first json object."""
        if self._started:
            return
        self._started = True
        status_text = httplib.responses[status]
        self._write = self._start_response('%d %s' % (status, status_text),
                                         [('content-type', self.content_type),
                                          ('cache-control', 'no-cache')] +
                                             headers.items())
        # xxx version in headers
        if obj_dic is not None:
            self._no_initial_obj = False
            self._write(simplejson.dumps(obj_dic) + "\r\n")

    def finish_response(self):
        """finish sending response."""
        self.sent_response = True

    def send_response_json(self, status=200, headers={}, **kwargs):
        """send and finish response with json object body from keyword args."""
        self.start_response(status, kwargs, headers)
        self.finish_response()

    def send_response_content(self, content, headers={}):
        """send and finish response with content"""
        headers['content-length'] = str(len(content))
        self.start_response(200, headers=headers)
        self.content = [content]
        self.finish_response()

    def start_stream(self):
        "start stream (array) as part of the response."
        assert self._started and self._no_initial_obj
        self._stream_state = 0
        self._write("[")

    def stream_entry(self, entry):
        "send stream entry as part of the response."
        assert self._stream_state != -1
        if self._stream_state == 0:
            self._stream_state = 1
            self._write('\r\n')
        else:
            self._write(',\r\n')
        self._write(simplejson.dumps(entry))

    def end_stream(self):
        "end stream (array)."
        assert self._stream_state != -1
        self._write("\r\n]\r\n")


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
            args = dict((k.decode('utf-8'), v.decode('utf-8')) for k, v in args)
        except ValueError:
            raise BadRequest()
        method = self.environ['REQUEST_METHOD'].lower()
        if method in ('get', 'delete'):
            meth = self._lookup(method)
            return meth(args, None)
        else:
            # we expect content-length > 0, reconsider if we move
            # to support chunked enconding
            try:
                content_length = int(self.environ['CONTENT_LENGTH'])
            except (ValueError, KeyError), e:
                raise BadRequest
            if content_length <= 0:
                raise BadRequest
            reader = _FencedReader(self.environ['wsgi.input'], content_length)
            content_type = self.environ.get('CONTENT_TYPE')
            if content_type == 'application/json':
                meth = self._lookup(method)
                body = reader.read_chunk(sys.maxint)
                return meth(args, body)
            elif content_type == 'application/x-u1db-sync-stream':
                meth_args = self._lookup('%s_args' % method)
                meth_entry = self._lookup('%s_stream_entry' % method)
                meth_end = self._lookup('%s_end' % method)
                body_getline = reader.getline
                if body_getline().strip() != '[':
                    raise BadRequest
                line = body_getline()
                line, comma = utils.check_and_strip_comma(line.strip())
                meth_args(args, line)
                while True:
                    line = body_getline()
                    entry = line.strip()
                    if entry == ']':
                        break
                    if not entry or not comma:  # empty or no prec comma
                        raise BadRequest
                    entry, comma = utils.check_and_strip_comma(entry)
                    meth_entry({}, entry)
                if comma or body_getline():  # extra comma or data
                    raise BadRequest
                return meth_end()
            else:
                raise BadRequest()


class HTTPApp(object):

    def __init__(self, state):
        self.state = state

    def _lookup_resource(self, environ, responder):
        resource_cls, params = url_to_resource.match(environ['PATH_INFO'])
        if resource_cls is None:
            raise BadRequest  # 404 instead?
        resource = resource_cls(state=self.state, responder=responder, **params)
        return resource

    def __call__(self, environ, start_response):
        responder = HTTPResponder(start_response)
        self.request_begin(environ)
        try:
            resource = self._lookup_resource(environ, responder)
            HTTPInvocationByMethodWithBody(resource, environ)()
        except errors.U1DBError, e:
            self.request_u1db_error(environ, e)
            status = http_errors.wire_description_to_status.get(
                                                            e.wire_description,
                                                            500)
            responder.send_response_json(status, error=e.wire_description)
        except BadRequest:
            self.request_bad_request(environ)
            responder.send_response_json(400, error="bad request")
        except KeyboardInterrupt:
            raise
        except:
            self.request_failed(environ)
            raise
        else:
            self.request_done(environ)
        return responder.content

    # hooks for tracing requests

    def request_begin(self, environ):
        """Hook called at the beginning of processing a request."""
        pass

    def request_done(self, environ):
        """Hook called when done processing a request."""
        pass

    def request_u1db_error(self, environ, exc):
        """Hook called when processing a request resulted in a U1DBError.

        U1DBError passed as exc.
        """
        pass

    def request_bad_request(self, environ):
        """Hook called when processing a bad request.

        No actual processing was done.
        """
        pass

    def request_failed(self, environ):
        """Hook called when processing a request failed unexpectedly.

        Invoked from an except block, so there's interpreter exception
        information available.
        """
        pass
