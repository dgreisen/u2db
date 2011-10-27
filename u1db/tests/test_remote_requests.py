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
    tests,
    )
from u1db.remote import (
    requests,
    )


class TestRPCRequest(tests.TestCase):

    def test_register_request(self):
        class MyRequest(requests.RPCRequest):
            name = 'mytestreq'
        reqs = requests.RPCRequest.requests
        self.assertIs(None, reqs.get('mytestreq'))
        MyRequest.register()
        self.addCleanup(MyRequest.unregister)
        self.assertEqual(MyRequest, reqs.get('mytestreq'))
        MyRequest.unregister()
        self.assertIs(None, reqs.get('mytestreq'))
        # Calling it again should not be an error.
        MyRequest.unregister()

    def test_get_version_rpc(self):
        factory = requests.RPCRequest.requests.get('version')
        self.assertEqual(requests.RPCServerVersion, factory)
        self.assertEqual('version', factory.name)
        instance = factory()
        self.assertEqual('version', instance.name)
        # 'version' doesn't require arguments, it just returns the response
        self.assertIsNot(None, instance.response)
        self.assertEqual({'version': _u1db_version},
                         instance.response.response_kwargs)


class TestRequestState(tests.TestCase):

    def setUp(self):
        super(TestRequestState, self).setUp()
        self.state = requests.RequestState()

    def test_set_workingdir(self):
        tempdir = self.createTempDir()
        self.state.set_workingdir(tempdir)
        self.assertTrue(self.state._relpath('path').startswith(tempdir))

    # def test_open_database(self):
    #     tempd
    #     db = self.state.open_database('/:memory:')
