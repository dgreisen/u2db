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
    remote_requests,
    tests,
    )


class TestRPCRequest(tests.TestCase):

    def test_register_request(self):
        class MyRequest(remote_requests.RPCRequest):
            name = 'mytestreq'
        self.assertIs(None, remote_requests.RPCRequest.lookup('mytestreq'))
        MyRequest.register()
        self.addCleanup(MyRequest.unregister)
        self.assertEqual(MyRequest,
                         remote_requests.RPCRequest.lookup('mytestreq'))
        MyRequest.unregister()
        self.assertIs(None, remote_requests.RPCRequest.lookup('mytestreq'))
        # Calling it again should not be an error.
        MyRequest.unregister()
