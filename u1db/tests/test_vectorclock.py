# Copyright (C) 2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""VectorClockRev helper class tests."""

from u1db import tests, vectorclock


class TestVectorClockRev(tests.TestCase):

    def assertIsNewer(self, newer_rev, older_rev):
        new_vcr = vectorclock.VectorClockRev(newer_rev)
        old_vcr = vectorclock.VectorClockRev(older_rev)
        self.assertTrue(new_vcr.is_newer(old_vcr))
        self.assertFalse(old_vcr.is_newer(new_vcr))

    def assertIsConflicted(self, rev_a, rev_b):
        vcr_a = vectorclock.VectorClockRev(rev_a)
        vcr_b = vectorclock.VectorClockRev(rev_b)
        self.assertFalse(vcr_a.is_newer(vcr_b))
        self.assertFalse(vcr_b.is_newer(vcr_a))

    def test__is_newer_doc_rev(self):
        self.assertIsNewer('test:1', None)
        self.assertIsNewer('test:2', 'test:1')
        self.assertIsNewer('test:1|other:2', 'test:1|other:1')
        self.assertIsNewer('test:1|other:1', 'other:1')
        self.assertIsConflicted('test:1|other:2', 'test:2|other:1')
        self.assertIsConflicted('test:1|other:1', 'other:2')
        self.assertIsConflicted('test:1', 'test:1')

    def test__expand_None(self):
        vcr = vectorclock.VectorClockRev(None)
        self.assertEqual({}, vcr._expand())
        vcr = vectorclock.VectorClockRev('')
        self.assertEqual({}, vcr._expand())

    def test__expand(self):
        vcr = vectorclock.VectorClockRev('test:1')
        self.assertEqual({'test': 1}, vcr._expand())
        vcr = vectorclock.VectorClockRev('other:2|test:1')
        self.assertEqual({'other': 2, 'test': 1}, vcr._expand())

    def assertIncrement(self, original, machine_id, after_increment):
        vcr = vectorclock.VectorClockRev(original)
        self.assertEqual(after_increment, vcr.increment(machine_id))

    def test_increment(self):
        self.assertIncrement(None, 'test', 'test:1')
        self.assertIncrement('test:1', 'test', 'test:2')
        self.assertIncrement('other:1', 'test', 'other:1|test:1')

    def assertMaximize(self, rev1, rev2, maximized):
        self.assertEqual(maximized,
                         vectorclock.VectorClockRev(rev1).maximize(rev2))
        self.assertEqual(maximized,
                         vectorclock.VectorClockRev(rev2).maximize(rev1))

    def test_maximize(self):
        self.assertMaximize(None, None, '')
        self.assertMaximize(None, 'x:1', 'x:1')
        self.assertMaximize('x:1', 'y:1', 'x:1|y:1')
        self.assertMaximize('x:2', 'x:1', 'x:2')
        self.assertMaximize('x:2', 'x:1|y:2', 'x:2|y:2')
