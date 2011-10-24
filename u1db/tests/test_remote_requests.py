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

"""Tests for the remote request objects"""

from u1db import (
    __version__ as _u1db_version,
    remote_requests,
    tests,
    )


class TestRPCRequest(tests.TestCase):

    def test_register_request(self):
        class MyRequest(remote_requests.RPCRequest):
            name = 'mytestreq'
        requests = remote_requests.RPCRequest.requests
        self.assertIs(None, requests.get('mytestreq'))
        MyRequest.register()
        self.addCleanup(MyRequest.unregister)
        self.assertEqual(MyRequest, requests.get('mytestreq'))
        MyRequest.unregister()
        self.assertIs(None, requests.get('mytestreq'))
        # Calling it again should not be an error.
        MyRequest.unregister()

    def test_get_version_rpc(self):
        factory = remote_requests.RPCRequest.requests.get('version')
        self.assertEqual(remote_requests.RPCServerVersion, factory)
        self.assertEqual('version', factory.name)
        instance = factory()
        self.assertEqual('version', instance.name)
        # 'version' doesn't require arguments, it just returns the response
        self.assertIsNot(None, instance.response)
        self.assertEqual({'version': _u1db_version},
                         instance.response.response_kwargs)
