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

"""VectorClockRev helper class."""


class VectorClockRev(object):
    """Track vector clocks for multiple machine ids.

    This allows simple comparison to determine if one VectorClockRev is
    newer/older/in-conflict-with another VectorClockRev without having to
    examine history. Every machine has a strictly increasing revision. When
    creating a new revision, they include all revisions for all other machines
    which the new revision dominates, and increment their own revision to
    something greater than the current value.
    """

    def __init__(self, value):
        self._values = self._expand(value)

    def __repr__(self):
        s = self.as_str()
        return '%s(%s)' % (self.__class__.__name__, s)

    def as_str(self):
        s = '|'.join(['%s:%d' % (m,r) for m,r
                      in sorted(self._values.items())])
        return s

    def _expand(self, value):
        result = {}
        if value is None:
            return result
        for machine_info in value.split('|'):
            machine_id, counter = machine_info.split(':')
            counter = int(counter)
            result[machine_id] = counter
        return result

    def is_newer(self, other):
        """Is this VectorClockRev strictly newer than other.
        """
        if not self._values:
            return False
        if not other._values:
            return True
        this_is_newer = False
        other_expand = dict(other._values)
        for key, value in self._values.iteritems():
            if key in other_expand:
                other_value = other_expand.pop(key)
                if other_value > value:
                    return False
                elif other_value < value:
                    this_is_newer = True
            else:
                this_is_newer = True
        if other_expand:
            return False
        return this_is_newer

    def increment(self, machine_id):
        """Increase the 'machine_id' section of this vector clock.

        :return: A string representing the new vector clock value
        """
        self._values[machine_id] = self._values.get(machine_id, 0) + 1

    def maximize(self, other_vcr):
        for machine_id, counter in other_vcr._values.iteritems():
            if machine_id not in self._values:
                self._values[machine_id] = counter
            else:
                this_counter = self._values[machine_id]
                if this_counter < counter:
                    self._values[machine_id] = counter
