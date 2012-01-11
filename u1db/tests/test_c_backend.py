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


from u1db import tests
try:
    from u1db.tests import c_backend_wrapper
except ImportError:
    c_backend_wrapper = None


class TestCDatabase(tests.TestCase):

    def test_exists(self):
        if c_backend_wrapper is None:
            self.fail("Could not import the c_backend_wrapper module."
                      " Was it compiled properly?")


