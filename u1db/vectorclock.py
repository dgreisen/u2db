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
        self._value = value

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._value)

    def _expand(self):
        if not self._value:
            return {}
        result = {}
        for machine_info in self._value.split('|'):
            machine_id, counter = machine_info.split(':')
            counter = int(counter)
            result[machine_id] = counter
        return result

    def is_newer(self, other):
        """Is this VectorClockRev strictly newer than other.
        """
        if self._value is None:
            return False
        if other._value is None:
            return True
        this_expand = self._expand()
        other_expand = other._expand()
        this_is_newer = False
        for key, value in this_expand.iteritems():
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
        expanded = self._expand()
        expanded[machine_id] = expanded.get(machine_id, 0) + 1
        result = ['%s:%d' % (m, c) for m, c in sorted(expanded.items())]
        return '|'.join(result)

    def maximize(self, other_rev):
        other_vcr = VectorClockRev(other_rev)
        this_exp = self._expand()
        other_exp = other_vcr._expand()
        for machine_id, counter in other_exp.iteritems():
            if machine_id not in this_exp:
                this_exp[machine_id] = counter
            else:
                this_counter = this_exp[machine_id]
                if this_counter < counter:
                    this_exp[machine_id] = counter
        result = ['%s:%d' % (m, c) for m, c in sorted(this_exp.items())]
        return '|'.join(result)
