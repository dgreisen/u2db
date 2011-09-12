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

"""VectorClockRev helper class tests."""

from u1db import tests, vectorclock


class TestVectorClockRev(tests.TestCase):

    def create_vcr(self, rev):
        return vectorclock.VectorClockRev(rev)

    def assertIsNewer(self, newer_rev, older_rev):
        new_vcr = self.create_vcr(newer_rev)
        old_vcr = self.create_vcr(older_rev)
        self.assertTrue(new_vcr.is_newer(old_vcr))
        self.assertFalse(old_vcr.is_newer(new_vcr))

    def assertIsConflicted(self, rev_a, rev_b):
        vcr_a = self.create_vcr(rev_a)
        vcr_b = self.create_vcr(rev_b)
        self.assertFalse(vcr_a.is_newer(vcr_b))
        self.assertFalse(vcr_b.is_newer(vcr_a))

    def test__is_newer_doc_rev(self):
        self.assertIsNewer('test:1', None)
        self.assertIsNewer('test:2', 'test:1')
        self.assertIsNewer('other:2|test:1', 'other:1|test:1')
        self.assertIsNewer('other:1|test:1', 'other:1')
        self.assertIsConflicted('other:2|test:1', 'other:1|test:2')
        self.assertIsConflicted('other:1|test:1', 'other:2')
        self.assertIsConflicted('test:1', 'test:1')
        self.assertIsConflicted('', None)

    def test_None(self):
        vcr = self.create_vcr(None)
        self.assertEqual('', vcr.as_str())

    def assertIncrement(self, original, machine_id, after_increment):
        vcr = self.create_vcr(original)
        vcr.increment(machine_id)
        self.assertEqual(after_increment, vcr.as_str())

    def test_increment(self):
        self.assertIncrement(None, 'test', 'test:1')
        self.assertIncrement('test:1', 'test', 'test:2')
        self.assertIncrement('other:1', 'test', 'other:1|test:1')

    def assertMaximize(self, rev1, rev2, maximized):
        vcr1 = self.create_vcr(rev1)
        vcr2 = self.create_vcr(rev2)
        vcr1.maximize(vcr2)
        self.assertEqual(maximized, vcr1.as_str())
        # reset vcr1 to maximize the other way
        vcr1 = self.create_vcr(rev1)
        vcr2.maximize(vcr1)
        self.assertEqual(maximized, vcr2.as_str())

    def test_maximize(self):
        self.assertMaximize(None, None, '')
        self.assertMaximize(None, 'x:1', 'x:1')
        self.assertMaximize('x:1', 'y:1', 'x:1|y:1')
        self.assertMaximize('x:2', 'x:1', 'x:2')
        self.assertMaximize('x:2', 'x:1|y:2', 'x:2|y:2')
